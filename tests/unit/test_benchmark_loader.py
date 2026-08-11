from __future__ import annotations

import json

from benchmarks.loader import load_locomo


def test_loader_keeps_image_only_turn_and_caption_provenance(tmp_path) -> None:
    source = tmp_path / "locomo.json"
    source.write_text(
        json.dumps(
            [
                {
                    "sample_id": "sample-1",
                    "conversation": {
                        "speaker_a": "A",
                        "speaker_b": "B",
                        "session_1_date_time": "1:56 pm on 8 May, 2023",
                        "session_1": [
                            {
                                "speaker": "A",
                                "dia_id": "D1:1",
                                "text": "",
                                "blip_caption": "a red bicycle",
                                "img_url": ["https://example.test/bike.jpg"],
                                "query": "red bicycle",
                            }
                        ],
                    },
                    "qa": [],
                }
            ]
        ),
        encoding="utf-8",
    )

    turn = load_locomo(source)[0].turns[0]
    assert turn.dia_id == "D1:1"
    assert turn.has_image is True
    assert turn.blip_caption == "a red bicycle"
    assert turn.image_urls == ["https://example.test/bike.jpg"]
    assert "[Image caption: a red bicycle]" in turn.attributed()
