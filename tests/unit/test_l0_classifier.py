"""Runtime L0 classifier configuration and safe-loading tests."""

from pathlib import Path

from engram.config import GatingConfig
from engram.retrieval.l0_classifier import load_l0_classifier


def test_off_mode_does_not_require_artifact(tmp_path: Path):
    classifier, status = load_l0_classifier(
        GatingConfig(classifier_mode="off", classifier_path=str(tmp_path / "absent"))
    )
    assert classifier.predict("anything") == 0.0
    assert status.to_dict() == {
        "mode": "off",
        "loaded": False,
        "degraded": False,
        "version": None,
        "artifact_sha256": None,
        "configured_path": str(tmp_path / "absent"),
        "error": None,
    }


def test_active_missing_artifact_is_degraded_but_does_not_raise(tmp_path: Path):
    classifier, status = load_l0_classifier(
        GatingConfig(classifier_mode="active", classifier_path=str(tmp_path / "absent"))
    )
    assert classifier.predict("anything") == 0.0
    assert status.mode == "active"
    assert status.loaded is False
    assert status.degraded is True
    assert status.error == "FileNotFoundError"


def test_shadow_missing_artifact_is_observable_without_degrading_service(tmp_path: Path):
    _classifier, status = load_l0_classifier(
        GatingConfig(classifier_mode="shadow", classifier_path=str(tmp_path / "absent"))
    )
    assert status.loaded is False
    assert status.degraded is False
    assert status.error == "FileNotFoundError"
