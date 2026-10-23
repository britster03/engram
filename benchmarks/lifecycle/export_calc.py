"""Export a spreadsheet-sized management CSV without losing retrieval details."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path


COLUMN = "Retrieved Data / Frontier Context"
MAX_CELL_CHARS = 12_000


def _compact_context(value, details_file, preview_chars):
    sources = {}
    for source in ("postgresql", "neo4j", "redis"):
        sources[source] = [
            {
                "stage": read.get("stage"),
                "store": read.get("store"),
                "operation": read.get("operation"),
                "read_count": read.get("read_count"),
                "returned_count": read.get("returned_count"),
                "distinct_result_count": read.get("distinct_result_count"),
                "sample_items_preview": json.dumps(
                    read.get("items", [])[:1], ensure_ascii=False
                )[:preview_chars],
            }
            for read in value.get(source, [])
        ]
    frontier_calls = [
        {
            "phase": call.get("phase"),
            "model": call.get("model"),
            "submitted": call.get("submitted"),
            "attempt_statuses": call.get("attempt_statuses"),
            "context_chars": len(call.get("context", "")),
            "context_sha256": hashlib.sha256(
                call.get("context", "").encode("utf-8")
            ).hexdigest(),
            "context_preview": call.get("context", "")[:preview_chars],
        }
        for call in value.get("frontier_calls", [])
    ]
    return {
        **sources,
        "frontier_calls": frontier_calls,
        "answer_mode": value.get("answer_mode"),
        "trace_id": value.get("trace_id"),
        "trace_archive": value.get("trace_archive"),
        "full_details_file": details_file,
    }


def compact_context(value, details_file):
    for preview_chars in (450, 250, 100, 0):
        result = json.dumps(
            _compact_context(value, details_file, preview_chars), ensure_ascii=False
        )
        if len(result) <= MAX_CELL_CHARS:
            return result
    raise ValueError("Retrieval metadata alone exceeds the spreadsheet cell budget")


def export(source, destination, details):
    source, destination, details = map(Path, (source, destination, details))
    if source.resolve() in {destination.resolve(), details.resolve()}:
        raise ValueError("Export destinations must differ from the source CSV")
    if destination.resolve() == details.resolve():
        raise ValueError("CSV and details destinations must differ")
    if destination.exists() or details.exists():
        raise FileExistsError("Export destination already exists")
    csv.field_size_limit(sys.maxsize)
    with source.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or COLUMN not in reader.fieldnames:
            raise ValueError(f"Source CSV is missing {COLUMN}")
        fieldnames = reader.fieldnames
        rows = list(reader)
    full_details = []
    compact_rows = []
    for number, row in enumerate(rows, 1):
        value = json.loads(row[COLUMN])
        full_details.append({
            "row": number,
            "conversation": row["Conversation"],
            "question": row["Question"],
            "retrieval_context": value,
        })
        compact_rows.append({
            **row,
            COLUMN: compact_context(value, details.name),
        })
    destination.parent.mkdir(parents=True, exist_ok=True)
    details.parent.mkdir(parents=True, exist_ok=True)
    with details.open("x", encoding="utf-8") as stream:
        json.dump(full_details, stream, ensure_ascii=False, indent=2)
    with destination.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(compact_rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("details", type=Path)
    args = parser.parse_args()
    print(f"Exported {export(args.source, args.destination, args.details)} rows")


if __name__ == "__main__":
    main()
