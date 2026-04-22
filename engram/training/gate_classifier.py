"""L0 gating classifier training — 33M BGE-Small head (§14.1).

Binary classifier over the query surface form. Accepts data in the flat
`{query, label}` shape produced by
`engram.training.synthetic_data --tasks gate_classifier`.

Heavy ML deps are imported lazily. Install with:

    pip install 'engram[training]'

Usage:
    python -m engram.training.synthetic_data --tasks gate_classifier --out ./data/train
    python -m engram.training.gate_classifier \
        --data ./data/train/gate_classifier.jsonl \
        --out ./models/engram-gate-v1
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_dataset(path: Path) -> list[dict]:
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if "query" not in rec or "label" not in rec:
            raise ValueError(f"bad record (needs query + label): {rec}")
        if rec["label"] not in (0, 1):
            raise ValueError(f"label must be 0 or 1, got {rec['label']}")
        rows.append(rec)
    return rows


def train(
    data_path: Path, out_dir: Path, *,
    epochs: int = 5, lr: float = 2e-5, batch: int = 64, max_len: int = 128,
) -> None:
    try:
        import torch  # type: ignore
        from torch.utils.data import DataLoader, Dataset  # type: ignore
        from transformers import (  # type: ignore
            AutoModel, AutoTokenizer, get_cosine_schedule_with_warmup,
        )
    except ImportError as err:
        raise SystemExit(
            "Training deps not installed. Run:\n"
            "    pip install 'engram[training]'\n"
            f"(missing: {err.name})"
        ) from err

    rows = load_dataset(data_path)
    if not rows:
        raise SystemExit(f"no records in {data_path}")
    print(f"loaded {len(rows)} labeled queries")

    tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-small-en-v1.5")
    backbone = AutoModel.from_pretrained("BAAI/bge-small-en-v1.5")
    head = torch.nn.Linear(384, 2)

    class GateDS(Dataset):
        def __len__(self) -> int: return len(rows)
        def __getitem__(self, i: int) -> dict:
            r = rows[i]
            enc = tokenizer(
                r["query"], truncation=True, max_length=max_len, return_tensors="pt",
            )
            return {
                "input_ids": enc.input_ids.squeeze(0),
                "attention_mask": enc.attention_mask.squeeze(0),
                "label": int(r["label"]),
            }

    def collate(batch_: list[dict]) -> dict:
        from torch.nn.utils.rnn import pad_sequence  # type: ignore
        ids = pad_sequence([b["input_ids"] for b in batch_], batch_first=True)
        mask = pad_sequence([b["attention_mask"] for b in batch_], batch_first=True)
        labels = torch.tensor([b["label"] for b in batch_])
        return {"input_ids": ids, "attention_mask": mask, "labels": labels}

    loader = DataLoader(GateDS(), batch_size=batch, shuffle=True, collate_fn=collate)
    optim = torch.optim.AdamW(
        list(backbone.parameters()) + list(head.parameters()),
        lr=lr, weight_decay=0.01,
    )
    total_steps = len(loader) * epochs
    sched = get_cosine_schedule_with_warmup(
        optim, int(total_steps * 0.1), total_steps,
    )
    weights = torch.tensor([1.0, 1.5])
    loss_fn = torch.nn.CrossEntropyLoss(weight=weights)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    backbone.to(device); head.to(device)
    for epoch in range(epochs):
        backbone.train(); head.train()
        for step, batch_ in enumerate(loader):
            batch_ = {k: v.to(device) for k, v in batch_.items()}
            outputs = backbone(
                input_ids=batch_["input_ids"],
                attention_mask=batch_["attention_mask"],
            )
            cls = outputs.last_hidden_state[:, 0]
            logits = head(cls)
            loss = loss_fn(logits, batch_["labels"])
            loss.backward()
            optim.step(); sched.step(); optim.zero_grad()
            if step % 50 == 0:
                print(f"epoch={epoch} step={step} loss={loss.item():.4f}")

    out_dir.mkdir(parents=True, exist_ok=True)
    backbone.save_pretrained(out_dir)
    tokenizer.save_pretrained(out_dir)
    torch.save(head.state_dict(), out_dir / "head.pt")
    print(f"saved classifier to {out_dir}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--epochs", type=int, default=5)
    parser.add_argument("--lr", type=float, default=2e-5)
    parser.add_argument("--batch", type=int, default=64)
    parser.add_argument("--max-len", type=int, default=128)
    args = parser.parse_args(argv)
    train(
        args.data, args.out,
        epochs=args.epochs, lr=args.lr, batch=args.batch, max_len=args.max_len,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
