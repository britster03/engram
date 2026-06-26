"""Local Core Model provider backed by Hugging Face transformers.

This is intentionally lazy: building app state should not import torch or
load a multi-GB model. The first Core call loads `core_model.model_path`,
which can be either a full causal-LM checkpoint or a PEFT/LoRA adapter path
when the optional training dependencies are installed.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from engram.config import CoreModelConfig
from engram.models.core import CompletionResult, CoreModelError, CoreModelProvider


class LocalCoreProvider(CoreModelProvider):
    def __init__(self, cfg: CoreModelConfig) -> None:
        self.cfg = cfg
        self._tokenizer: Any | None = None
        self._model: Any | None = None

    def complete(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        output_schema: dict[str, Any] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> CompletionResult:
        started = time.perf_counter()
        tokenizer, model = self._load()
        prompt = _format_prompt(tokenizer, system_prompt, user_prompt)
        try:
            inputs = tokenizer(prompt, return_tensors="pt")
            if hasattr(model, "device"):
                inputs = {k: v.to(model.device) for k, v in inputs.items()}
            temp = self.cfg.temperature if temperature is None else temperature
            generate_kwargs: dict[str, Any] = {
                **inputs,
                "max_new_tokens": max_tokens or self.cfg.max_tokens,
                "do_sample": temp > 0,
                "pad_token_id": tokenizer.eos_token_id,
            }
            if temp > 0:
                generate_kwargs["temperature"] = temp
            generated = model.generate(**generate_kwargs)
            prompt_tokens = int(inputs["input_ids"].shape[-1])
            raw_text = tokenizer.decode(
                generated[0][prompt_tokens:], skip_special_tokens=True
            ).strip()
            output = CoreModelProvider.extract_json(raw_text)
            return CompletionResult(
                output=output,
                raw_text=raw_text,
                tokens_in=prompt_tokens,
                tokens_out=max(0, int(generated.shape[-1]) - prompt_tokens),
                latency_ms=(time.perf_counter() - started) * 1000,
            )
        except CoreModelError:
            raise
        except Exception as err:
            raise CoreModelError(f"local core model call failed: {err}") from err

    def _load(self) -> tuple[Any, Any]:
        if self._tokenizer is not None and self._model is not None:
            return self._tokenizer, self._model
        try:
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except Exception as err:
            raise CoreModelError(
                "local core provider requires optional training dependencies; "
                "install with `pip install -e '.[training]'`"
            ) from err

        path = self.cfg.model_path
        tokenizer = AutoTokenizer.from_pretrained(path, trust_remote_code=True)
        model: Any
        if _looks_like_peft_adapter(path):
            try:
                from peft import AutoPeftModelForCausalLM

                model = AutoPeftModelForCausalLM.from_pretrained(
                    path, torch_dtype="auto", device_map="auto", trust_remote_code=True
                )
            except Exception as err:
                raise CoreModelError(f"failed to load local PEFT adapter {path!r}: {err}") from err
        else:
            try:
                model = AutoModelForCausalLM.from_pretrained(
                    path, torch_dtype="auto", device_map="auto", trust_remote_code=True
                )
            except TypeError:
                model = AutoModelForCausalLM.from_pretrained(path, trust_remote_code=True)
        model.eval()
        self._tokenizer = tokenizer
        self._model = model
        return tokenizer, model


def _looks_like_peft_adapter(model_path: str) -> bool:
    try:
        return (Path(model_path) / "adapter_config.json").exists()
    except Exception:
        return False


def _format_prompt(tokenizer: Any, system_prompt: str, user_prompt: str) -> str:
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    if getattr(tokenizer, "chat_template", None):
        return tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )
    return (
        "System:\n"
        f"{system_prompt}\n\n"
        "User:\n"
        f"{user_prompt}\n\n"
        "Assistant:\n"
    )
