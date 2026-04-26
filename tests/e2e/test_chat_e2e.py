import pytest
from playwright.sync_api import Page

pytestmark = pytest.mark.e2e


@pytest.fixture
def admin_logged_in_page(page: Page, base_url: str, admin_key: str):
    page.goto(f"{base_url}/admin/login")
    page.wait_for_selector('input[type="password"]', state="visible", timeout=30000)
    page.locator('input[type="password"]').fill(admin_key)
    page.locator('button[type="submit"]').click()
    page.wait_for_url(f"{base_url}/admin/dashboard", timeout=30000)
    yield page


@pytest.mark.e2e
def test_chat_page_loads_and_sends_message(admin_logged_in_page: Page, base_url: str):
    page = admin_logged_in_page
    page.goto(f"{base_url}/admin/chat")
    page.wait_for_selector('input[type="text"]', state="visible", timeout=30000)

    session_id = page.evaluate(
        "window.localStorage.getItem('engram_chat_session_id')"
    )
    assert session_id is not None
    assert isinstance(session_id, str)
    assert session_id.startswith("sess-")

    page.locator('input[type="text"]').fill("What is Engram?")
    page.locator('button[type="submit"]').click()

    page.wait_for_function(
        """() => {
            const last = document.querySelector('#chat-history > div:last-child');
            if (!last) return false;
            const p = last.querySelector('p');
            return p && p.textContent.trim().length > 0;
        }""",
        timeout=30000,
    )

    response_text: str = page.evaluate(
        """() => {
            const last = document.querySelector('#chat-history > div:last-child');
            return last ? last.querySelector('p').textContent : '';
        }"""
    )
    assert len(response_text.strip()) > 0
