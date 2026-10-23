"""Official QA scores plus separately labelled answer-similarity diagnostics."""
from __future__ import annotations

import contextlib
import functools
import hashlib
import io
import json
import os
import sys
import time
import uuid
from pathlib import Path

from .ledger import append, atomic, read
from .vendor import verify_official_scorer
from .vendor.official_qa import eval_question_answering, normalize_answer

verify_official_scorer()

BERT_MODEL_REVISION = "722cf37b1afa9454edce342e7895e588b6ff1d59"


def observe_local_encoder(model,directory,batch_ids):
    """Count actual encoder positions, separately from provider token usage."""
    original=model.forward
    @functools.wraps(original)
    def forward(*args,**kwargs):
        ids=kwargs.get("input_ids") if "input_ids" in kwargs else args[0]
        mask=kwargs.get("attention_mask")
        record={"attempt_id":uuid.uuid4().hex,"model":"FacebookAI/roberta-large","evaluation_ids":list(batch_ids),"input_token_positions":int(ids.numel()),"unpadded_input_tokens":int(mask.sum().item()) if mask is not None else None,"generated_tokens":0,"count_source":"actual_encoder_tensors","provider_billable_tokens":0}
        append(Path(directory)/"local_scoring_attempts.jsonl",{**record,"state":"STARTED"})
        # Keep recovery writable even if inference is killed before finally.
        restore_output_ownership(directory)
        started=time.perf_counter()
        try:
            result=original(*args,**kwargs)
            record["status"]="RETURNED"
            return result
        except BaseException as error:
            record.update(status="FAILED",error_type=type(error).__name__)
            raise
        finally:
            append(Path(directory)/"local_scoring_attempts.jsonl",{**record,"state":"FINISHED","latency_ms":(time.perf_counter()-started)*1000})
    model.forward=forward


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda:stream.read(1024*1024),b""):
            h.update(block)
    return h.hexdigest()


def score(prediction, answer, category, *, operational_status="COMPLETE"):
    if operational_status != "COMPLETE":
        return {"official_locomo_f1": 0.0, "exact_match": 0.0, "raw_exact_match": 0.0, "adversarial_correct": 0.0 if category == 5 else None, "secondary_metric_status": "OPERATIONAL_FAILURE", "rouge_l": None, "bertscore_f1": None, "composite_similarity": None}
    with contextlib.redirect_stdout(io.StringIO()):
        values, _, _ = eval_question_answering([{"prediction": prediction, "answer": answer, "category": category, "evidence": []}])
    gold = str(answer).split(";")[0].strip() if category == 3 else str(answer)
    result = {"official_locomo_f1": float(values[0]), "exact_match": float(normalize_answer(prediction) == normalize_answer(gold)), "raw_exact_match": float(prediction == gold), "adversarial_correct": float(values[0]) if category == 5 else None, "secondary_metric_status": "NOT_APPLICABLE_ADVERSARIAL" if category == 5 else "PENDING", "rouge_l": None, "bertscore_f1": None, "composite_similarity": None}
    if category != 5:
        from rouge_score.rouge_scorer import RougeScorer
        result["rouge_l"] = RougeScorer(["rougeL"], use_stemmer=True).score(gold, prediction)["rougeL"].fmeasure
    return result


def secondary(directory):
    directory = Path(directory)
    rows = read(directory/"result_rows.jsonl")
    eligible = [r for r in rows if r["benchmark_track"] != "SEARCH_COMPARISON" and r["status"] == "COMPLETE" and r["category_id"] != 5]
    output_path=directory/"secondary_scores.json"
    output=json.loads(output_path.read_text()) if output_path.exists() else {}
    eligible=[r for r in eligible if r["evaluation_id"] not in output]
    if not eligible:
        atomic(directory/"secondary_scores.json", output)
        return
    manifest_path = directory/"secondary_model.json"
    try:
        import bert_score
        from bert_score import BERTScorer
        from huggingface_hub import snapshot_download
        if manifest_path.exists():
            revision = json.loads(manifest_path.read_text())["revision"]
        else:
            revision = BERT_MODEL_REVISION
        path = snapshot_download("FacebookAI/roberta-large", revision=revision, allow_patterns=["config.json", "pytorch_model.bin", "vocab.json", "merges.txt", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json"])
        baseline = Path(bert_score.__file__).parent/"rescale_baseline/en/roberta-large.tsv"
        scorer = BERTScorer(model_type=path, num_layers=17, idf=False, rescale_with_baseline=True, baseline_path=str(baseline), lang="en", device="cpu", batch_size=4)
        batch_ids=[]
        observe_local_encoder(scorer._model,directory,batch_ids)
        atomic(manifest_path, {"model": "FacebookAI/roberta-large", "revision": revision, "num_layers": 17, "idf": False, "rescale_with_baseline": True,"scoring_batch_size":4, "baseline_sha256": file_sha(baseline), "model_files": {p.name: file_sha(p) for p in Path(path).iterdir() if p.is_file()}})
        for offset in range(0, len(eligible), 16):
            group = eligible[offset:offset+16]
            batch_ids[:]=[r["evaluation_id"] for r in group]
            _, _, f1 = scorer.score([r["prediction"] for r in group], [str(r["gold_answer"]).split(";")[0].strip() if r["category_id"] == 3 else str(r["gold_answer"]) for r in group],batch_size=4)
            for row, value in zip(group, f1.tolist(), strict=True):
                output[row["evaluation_id"]] = {"bertscore_f1": value, "composite_similarity": .4*row["rouge_l"] + .6*max(0, min(1, value)),"bertscore_batch_size":4, "secondary_metric_status": "COMPLETE"}
            atomic(directory/"secondary_scores.json", output)
    except Exception as error:
        atomic(directory/"secondary_failure.json", {"status": "UNAVAILABLE", "error_type": type(error).__name__, "evaluated": len(output), "expected": len(eligible)})
        raise


def restore_output_ownership(directory):
    """The CPU scoring image runs as root; outputs belong to the run owner."""
    if os.geteuid()!=0:
        return
    directory=Path(directory)
    owner=directory.stat()
    for name in ("local_scoring_attempts.jsonl","secondary_scores.json","secondary_model.json","secondary_failure.json"):
        path=directory/name
        if path.is_file() and not path.is_symlink():
            os.chown(path,owner.st_uid,owner.st_gid)


if __name__ == "__main__":
    try:
        secondary(sys.argv[1])
    finally:
        restore_output_ownership(sys.argv[1])
