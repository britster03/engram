"""Validate extraction coordinates; temporal anchors come from original turns."""

from __future__ import annotations

import re
from datetime import datetime


def source_context(payload):
    pair = payload.get("turn_pair") or payload.get("turn_group") or payload
    return {
        role: {
            "timestamp": value.get("timestamp"),
            "turn_idx": value.get("turn_idx"),
            "speaker": value.get("speaker"),
            "content": value.get("content", ""),
        }
        for role in ("user", "assistant")
        if isinstance(value := pair.get(role), dict)
    }


def validate_extraction(extraction, sources):
    """Reject invented spans and preserve temporal precision without changing assertion time."""
    output = dict(extraction)
    triplets = []
    for raw in extraction.get("triplets", []):
        span = raw.get("source_span") or {}
        role, quote = span.get("role"), span.get("quote")
        source = sources.get(role)
        if (
            not source
            or not isinstance(quote, str)
            or not quote.strip()
            or quote not in source["content"]
        ):
            raise ValueError("extraction must quote an exact original message span")
        matches = [m.start() for m in re.finditer(re.escape(quote), source["content"])]
        supplied_start = span.get("start")
        if len(matches) > 1 and supplied_start not in matches:
            raise ValueError("repeated source quotation requires an exact start offset")
        start = supplied_start if supplied_start in matches else matches[0]
        metadata = {
            "source_role": role,
            "source_timestamp": source.get("timestamp"),
            "source_speaker": source.get("speaker"),
            "temporal_precision": "unknown",
        }
        # Only unambiguous relative years are normalized automatically. Other
        # expressions remain source text; the query clock never supplies an anchor.
        try:
            anchor = datetime.fromisoformat(str(source.get("timestamp")).replace("Z", "+00:00"))
        except ValueError:
            anchor = None
        relatives = list(re.finditer(r"\b(last|this|next) year\b", quote, re.I))
        relative = relatives[0] if len(relatives) == 1 else None
        if anchor and relative:
            metadata.update(
                occurrence_year=anchor.year
                + {"last": -1, "this": 0, "next": 1}[relative[1].lower()],
                temporal_precision="year",
                temporal_anchor=source["timestamp"],
                relative_expression=relative[0],
            )
        triplets.append(
            {
                **raw,
                "source_span": {
                    "role": role,
                    "quote": quote,
                    "start": start,
                    "end": start + len(quote),
                    "turn_idx": source.get("turn_idx"),
                },
                "source_metadata": metadata,
            }
        )
    output["triplets"] = triplets
    return output
