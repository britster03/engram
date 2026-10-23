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
    page.wait_for_selector("#chat-history p", state="visible", timeout=30000)

    greeting = "Hello. I am the Engram admin assistant."
    assert page.locator("#chat-history p", has_text=greeting).count() == 1

    session_id = page.evaluate("window.localStorage.getItem('engram_chat_session_id')")
    assert session_id is not None
    assert isinstance(session_id, str)
    assert session_id.startswith("sess-")

    initial_count = page.locator("#chat-history > div p").count()
    page.locator('input[type="text"]').fill("What is Engram?")
    page.locator('button[type="submit"]').click()

    page.wait_for_function(
        """initialCount => {
            const paragraphs = Array.from(document.querySelectorAll('#chat-history > div p'));
            const last = paragraphs[paragraphs.length - 1];
            return paragraphs.length >= initialCount + 2
                && last
                && last.textContent.trim().length > 0
                && last.textContent.trim() !== 'What is Engram?';
        }""",
        arg=initial_count,
        timeout=30000,
    )

    response_text: str = page.evaluate(
        """() => {
            const paragraphs = Array.from(document.querySelectorAll('#chat-history > div p'));
            const last = paragraphs[paragraphs.length - 1];
            return last ? last.textContent : '';
        }"""
    )
    assert len(response_text.strip()) > 0


def test_chat_replaces_stale_browser_session(admin_logged_in_page: Page, base_url: str):
    page = admin_logged_in_page
    stale_session_id = "sess-stale-browser-id"

    page.goto(f"{base_url}/admin/chat")
    page.wait_for_selector('input[type="text"]', state="visible", timeout=30000)
    page.evaluate(
        """sessionId => window.localStorage.setItem('engram_chat_session_id', sessionId)""",
        stale_session_id,
    )
    page.reload()
    page.wait_for_selector('input[type="text"]', state="visible", timeout=30000)

    session_id = page.evaluate("window.localStorage.getItem('engram_chat_session_id')")
    assert session_id is not None
    assert session_id.startswith("sess-")
    assert session_id != stale_session_id


def test_chat_new_session_button_creates_and_renders_a_new_session(
    admin_logged_in_page: Page,
    base_url: str,
):
    page = admin_logged_in_page
    page.goto(f"{base_url}/admin/chat")
    page.wait_for_selector('input[type="text"]', state="visible", timeout=30000)

    first_session_id = page.evaluate("window.localStorage.getItem('engram_chat_session_id')")
    page.get_by_role("button", name="New Session").click()
    page.wait_for_function(
        """previousId => {
            const currentId = window.localStorage.getItem('engram_chat_session_id');
            return currentId && currentId !== previousId;
        }""",
        arg=first_session_id,
        timeout=30000,
    )

    new_session_id = page.evaluate("window.localStorage.getItem('engram_chat_session_id')")
    assert new_session_id is not None
    assert new_session_id.startswith("sess-")
    assert new_session_id != first_session_id
    assert page.get_by_text(f"New session started: {new_session_id}").is_visible()
