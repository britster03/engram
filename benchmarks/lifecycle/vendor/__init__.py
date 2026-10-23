"""Pinned upstream LoCoMo evaluation functions."""
import ast
import hashlib
import json
from pathlib import Path


def verify_official_scorer(base=None):
    """Reject missing functions as well as changes to the pinned scorer."""
    base = Path(base) if base is not None else Path(__file__).parent
    provenance = json.loads((base / "provenance.json").read_text())
    reference = (base / "evaluation.py.reference").read_bytes()
    if hashlib.sha256(reference).hexdigest() != provenance["sha256"]:
        raise RuntimeError("Official scorer reference checksum mismatch")
    required = set(provenance["functions"])
    def functions(source):
        return {node.name: ast.dump(node) for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef)}
    expected = functions(reference)
    actual = functions((base / "official_qa.py").read_text())
    if set(actual) != required or any(actual[name] != expected.get(name) for name in required):
        raise RuntimeError("Official scorer functions are missing or differ from upstream")
    return {"status": "VERIFIED", **provenance}
