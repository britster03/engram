"""Fetch a deterministic, hashed LoCoMo multimodal sample for local QA.

The LoCoMo JSON is the authoritative benchmark artifact. Image URLs point to
third-party hosts and can rot; this helper snapshots a bounded subset and
records every success/failure without treating external image rights as part
of the LoCoMo code/data license.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import random
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmarks.loader import load_locomo


@dataclass
class Candidate:
    sample_id: str
    dia_id: str
    url: str


def candidates(data_path: Path, *, seed: int) -> list[Candidate]:
    rows = [
        Candidate(conv.sample_id, turn.dia_id, url)
        for conv in load_locomo(data_path)
        for turn in conv.turns
        for url in turn.image_urls
    ]
    random.Random(seed).shuffle(rows)
    return rows


def _extension(content_type: str, data: bytes) -> str:
    media = content_type.split(";", 1)[0].strip().lower()
    known = {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
    }
    if media in known:
        return known[media]
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return ".gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return ".webp"
    raise ValueError("response is not a recognized image")


def _fetch(client: httpx.Client, url: str, *, max_bytes: int) -> tuple[bytes, str]:
    if url.startswith("data:image/"):
        header, encoded = url.split(",", 1)
        content_type = header[5:].split(";", 1)[0]
        data = base64.b64decode(encoded, validate=True)
    elif url.startswith(("http://", "https://")):
        with client.stream("GET", url) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise ValueError(f"image exceeds {max_bytes} byte cap")
                chunks.append(chunk)
            data = b"".join(chunks)
    else:
        raise ValueError("unsupported URL scheme")
    if not data:
        raise ValueError("empty image")
    return data, content_type


def fetch_sample(
    *,
    data_path: Path,
    output_dir: Path,
    target_bytes: int,
    seed: int,
    max_asset_bytes: int,
    timeout_s: float,
    upstream_commit: str,
) -> dict[str, Any]:
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=False)
    records: list[dict[str, Any]] = []
    downloaded = 0
    with httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(timeout_s),
        headers={"User-Agent": "Engram-LoCoMo-QA/1.0"},
    ) as client:
        for candidate in candidates(data_path, seed=seed):
            record = asdict(candidate)
            try:
                data, content_type = _fetch(
                    client, candidate.url, max_bytes=max_asset_bytes
                )
                extension = _extension(content_type, data)
                digest = hashlib.sha256(data).hexdigest()
                filename = f"{digest}{extension}"
                destination = assets_dir / filename
                if not destination.exists():
                    destination.write_bytes(data)
                    downloaded += len(data)
                record.update(
                    {
                        "status": "downloaded",
                        "sha256": digest,
                        "bytes": len(data),
                        "content_type": content_type,
                        "file": f"assets/{filename}",
                    }
                )
            except Exception as err:
                record.update(
                    {"status": "failed", "error": type(err).__name__}
                )
            records.append(record)
            if downloaded >= target_bytes:
                break

    manifest = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "target_bytes": target_bytes,
        "downloaded_bytes": downloaded,
        "dataset": {
            "path": str(data_path.resolve()),
            "sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
            "upstream": "https://github.com/snap-research/locomo",
            "upstream_commit": upstream_commit,
            "license": "CC BY-NC 4.0",
        },
        "rights_note": (
            "Third-party image URLs may carry separate copyright and terms; "
            "snapshots are for local noncommercial benchmark validation only."
        ),
        "records": records,
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("benchmarks/data/locomo10.json"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--target-mb", type=float, default=50.0)
    parser.add_argument("--max-asset-mb", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--upstream-commit", required=True)
    args = parser.parse_args()
    if args.target_mb <= 0 or args.max_asset_mb <= 0:
        parser.error("size limits must be positive")
    manifest = fetch_sample(
        data_path=args.data,
        output_dir=args.out,
        target_bytes=int(args.target_mb * 1024 * 1024),
        seed=args.seed,
        max_asset_bytes=int(args.max_asset_mb * 1024 * 1024),
        timeout_s=args.timeout,
        upstream_commit=args.upstream_commit,
    )
    successes = sum(row["status"] == "downloaded" for row in manifest["records"])
    failures = len(manifest["records"]) - successes
    print(
        f"downloaded={manifest['downloaded_bytes']} bytes "
        f"assets={successes} failures={failures} manifest={args.out / 'manifest.json'}"
    )
    return 0 if manifest["downloaded_bytes"] >= manifest["target_bytes"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
