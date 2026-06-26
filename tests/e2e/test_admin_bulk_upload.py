from uuid import uuid4

import pytest
from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login(page: Page, base_url: str, admin_key: str) -> None:
    page.goto(f"{base_url}/admin/login")
    page.wait_for_selector('input[type="password"]', state="visible", timeout=30000)
    page.locator('input[type="password"]').fill(admin_key)
    page.locator('button[type="submit"]').click()
    page.wait_for_url(f"{base_url}/admin/dashboard", timeout=30000)


@pytest.mark.e2e
def test_dashboard_bulk_upload_dry_run_posts_and_renders_result(
    page: Page,
    base_url: str,
    tmp_path,
    request,
) -> None:
    admin_key = request.getfixturevalue("admin_key")
    session_id = f"e2e-bulk-{uuid4().hex}"
    bulk_file = tmp_path / "turns.jsonl"
    bulk_file.write_text(
        (
            '{"turn_pair":{"user":{"content":"Bulk upload browser e2e",'
            '"turn_idx":0},"assistant":{"content":"Bulk upload browser e2e stored",'
            '"turn_idx":1}}}\n'
        ),
        encoding="utf-8",
    )

    _login(page, base_url, admin_key)
    page.get_by_role("button", name="Ingest").click()
    expect(page.get_by_role("heading", name="Ingest Pipeline")).to_be_visible()

    page.get_by_test_id("bulk-dry-run").check()
    page.get_by_test_id("bulk-file-input").set_input_files(str(bulk_file))
    page.get_by_test_id("bulk-session-id").fill(session_id)
    page.get_by_test_id("bulk-file-format").select_option("jsonl")

    with page.expect_response(
        lambda response: (
            "/admin/api/ingest/bulk" in response.url
            and response.request.method == "POST"
        ),
        timeout=30000,
    ) as response_info:
        page.get_by_test_id("bulk-upload-submit").click()

    response = response_info.value
    assert response.status == 202
    payload = response.json()
    assert payload["status"] == "DRY_RUN"
    assert payload["accepted_count"] == 1
    assert payload["rejected_count"] == 0
    assert payload["event_ids"] == []

    expect(page.get_by_test_id("bulk-status")).to_have_text(
        "DRY_RUN: 1 accepted, 0 rejected",
        timeout=30000,
    )
    expect(page.get_by_test_id("bulk-result-status")).to_have_text("DRY_RUN")
    expect(page.get_by_test_id("bulk-result-accepted")).to_have_text("1")
    expect(page.get_by_test_id("bulk-result-rejected")).to_have_text("0")
    expect(page.get_by_test_id("bulk-result-events")).to_have_text("0")
