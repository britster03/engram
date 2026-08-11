"""Runtime loader for the optional L0 BGE binary classifier.

The heavyweight training dependencies are imported only when shadow or active
mode is configured.  A broken or absent artifact never prevents Engram from
starting: the caller receives an unavailable status and L0 fails open when the
mode is active.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from engram.config import GatingConfig
from engram.retrieval.l0_gate import AlwaysClass0Classifier, L0Classifier


@dataclass(frozen=True)
class ClassifierStatus:
    mode: Literal["off", "shadow", "active"]
    loaded: bool
    degraded: bool
    version: str | None = None
    artifact_sha256: str | None = None
    configured_path: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BgeGateClassifier:
    """Inference adapter matching ``engram.training.gate_classifier``."""

    def __init__(self, tokenizer: Any, backbone: Any, head: Any, torch: Any, device: Any):
        self._tokenizer = tokenizer
        self._backbone = backbone
        self._head = head
        self._torch = torch
        self._device = device

    def predict(self, query: str) -> float:
        encoded = self._tokenizer(
            query,
            truncation=True,
            max_length=128,
            return_tensors="pt",
        )
        encoded = {key: value.to(self._device) for key, value in encoded.items()}
        with self._torch.inference_mode():
            outputs = self._backbone(**encoded)
            logits = self._head(outputs.last_hidden_state[:, 0])
            probability = self._torch.softmax(logits, dim=-1)[0, 1]
        return float(probability.detach().cpu().item())


def _artifact_version(path: Path, artifact_hash: str) -> str:
    metadata_path = path / "metadata.json"
    if metadata_path.is_file():
        try:
            value = json.loads(metadata_path.read_text(encoding="utf-8")).get("version")
            if isinstance(value, str) and value.strip():
                return value.strip()
        except (OSError, ValueError, TypeError):
            pass
    return f"{path.name}:{artifact_hash[:12]}"


def load_l0_classifier(cfg: GatingConfig) -> tuple[L0Classifier, ClassifierStatus]:
    """Load a trained gate artifact, returning status instead of raising."""
    if cfg.classifier_mode == "off":
        return AlwaysClass0Classifier(), ClassifierStatus(
            mode="off",
            loaded=False,
            degraded=False,
            configured_path=cfg.classifier_path,
        )

    artifact_dir = Path(cfg.classifier_path).expanduser()
    head_path = artifact_dir / "head.pt"
    try:
        if not artifact_dir.is_dir():
            raise FileNotFoundError("classifier directory is missing")
        if not head_path.is_file():
            raise FileNotFoundError("classifier head.pt is missing")

        artifact_hash = hashlib.sha256(head_path.read_bytes()).hexdigest()
        import torch  # type: ignore
        from transformers import AutoModel, AutoTokenizer  # type: ignore

        if cfg.device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("classifier configured for CUDA but CUDA is unavailable")
        device = torch.device(cfg.device)
        tokenizer = AutoTokenizer.from_pretrained(artifact_dir, local_files_only=True)
        backbone = AutoModel.from_pretrained(artifact_dir, local_files_only=True)
        hidden_size = int(backbone.config.hidden_size)
        head = torch.nn.Linear(hidden_size, 2)
        state_dict = torch.load(head_path, map_location=device, weights_only=True)
        head.load_state_dict(state_dict)
        backbone.to(device).eval()
        head.to(device).eval()
        return BgeGateClassifier(tokenizer, backbone, head, torch, device), ClassifierStatus(
            mode=cfg.classifier_mode,
            loaded=True,
            degraded=False,
            version=_artifact_version(artifact_dir, artifact_hash),
            artifact_sha256=artifact_hash,
            configured_path=cfg.classifier_path,
        )
    except Exception as err:
        # Do not publish paths or exception messages through the health API.
        # The exception class is enough to distinguish missing dependencies,
        # missing artifacts, and malformed weights operationally.
        return AlwaysClass0Classifier(), ClassifierStatus(
            mode=cfg.classifier_mode,
            loaded=False,
            degraded=cfg.classifier_mode == "active",
            configured_path=cfg.classifier_path,
            error=type(err).__name__,
        )
