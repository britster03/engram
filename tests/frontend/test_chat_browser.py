"""Exercise recovery with the real Alpine component in Chromium, without live writes."""

from __future__ import annotations

import json
import mimetypes
from pathlib import Path
from urllib.parse import urlparse

import pytest
from jinja2 import Environment, FileSystemLoader

playwright_api = pytest.importorskip("playwright.sync_api")
expect, sync_playwright = playwright_api.expect, playwright_api.sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_browser_busy_refresh_two_tabs_and_cancel_completion():
    env = Environment(loader=FileSystemLoader(ROOT / "engram/admin/templates"), autoescape=True)
    html = env.get_template("chat.html").render(
        current_lang="en",
        locale_json="{}",
        static=lambda name: "/admin/static/" + name,
        t=lambda name: name,
        version="test",
    )
    run = {
        "run_id": "run-browser",
        "client_message_id": "existing-message",
        "session_id": "sess-browser",
        "status": "RUNNING",
        "created_at": "2026-09-30T10:00:00Z",
    }
    phase = {"value": "idle"}
    posts, errors = [], []
    user = {
        "id": "turn:user:0",
        "role": "user",
        "content": "Who is Atul?",
        "client_message_id": run["client_message_id"],
    }
    answer = {
        "id": "turn:assistant:1",
        "role": "assistant",
        "content": "Atul is your colleague.",
        "run_id": run["run_id"],
    }

    def intercept(route):
        path = urlparse(route.request.url).path
        if path == "/admin/chat":
            route.fulfill(body=html, content_type="text/html")
            return
        if path.startswith("/admin/static/"):
            resource = ROOT / "engram/admin/static" / path.removeprefix("/admin/static/")
            if resource.is_file():
                route.fulfill(
                    body=resource.read_bytes(),
                    content_type=mimetypes.guess_type(resource)[0] or "application/octet-stream",
                )
            else:
                route.fulfill(status=404)
            return
        status = 200
        current = run | {"status": "COMPLETED" if phase["value"] == "completed" else "RUNNING"}
        if phase["value"] == "completed":
            current["response"] = {
                "answer": answer["content"],
                "retrieval_metadata": {"levels_visited": ["L1"]},
            }
        if path == "/admin/api/chat/completions":
            posts.append(route.request.post_data_json)
            phase["value"] = "running"
            status, body = (
                409,
                {
                    "detail": "a response is already running for this session",
                    "code": "session_busy",
                    "run": current,
                },
            )
        elif path.endswith("/cancel"):
            phase["value"] = "completed"
            body = run | {
                "status": "COMPLETED",
                "response": {"answer": answer["content"], "retrieval_metadata": {}},
            }
        elif path.endswith("/history"):
            body = {
                "status": "ACTIVE",
                "turns": []
                if phase["value"] == "idle"
                else [user, answer]
                if phase["value"] == "completed"
                else [user],
            }
        elif path.endswith("/chat-run"):
            body = {"run": None if phase["value"] == "idle" else current}
        else:
            body = current
        route.fulfill(status=status, body=json.dumps(body), content_type="application/json")

    with sync_playwright() as playwright:
        if not Path(playwright.chromium.executable_path).is_file():
            pytest.skip("Install Chromium with playwright install chromium to run browser checks")
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        context.add_init_script("localStorage.setItem('engram_chat_session_id', 'sess-browser')")
        context.route("http://engram.test/**", intercept)
        first, second = context.new_page(), context.new_page()
        for page in (first, second):
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto("http://engram.test/admin/chat")
            expect(page.locator("button[type=submit]")).to_be_enabled()
        second.locator("input[x-model=input]").fill("My next question")
        second.locator("button[type=submit]").click()
        expect(second.get_by_role("button", name="Stop response")).to_be_visible()
        expect(second.locator("button[type=submit]")).to_be_disabled()
        expect(second.locator("input[x-model=input]")).to_have_value("My next question")
        assert posts[0]["messages"] == [{"role": "user", "content": "My next question"}]
        second.reload()
        first.reload()
        for page in (first, second):
            expect(page.get_by_role("button", name="Stop response")).to_be_visible()
            expect(page.locator("p", has_text="Who is Atul?")).to_have_count(1)
        expect(second.locator("input[x-model=input]")).to_have_value("My next question")
        phase["value"] = "completed"
        for page in (first, second):
            expect(page.locator("button[type=submit]")).to_be_enabled(timeout=6000)
            expect(page.locator("p", has_text=answer["content"])).to_have_count(1)
        assert len(posts) == 1
        phase["value"] = "running"
        second.reload()
        second.get_by_role("button", name="Stop response").click()
        expect(second.locator("button[type=submit]")).to_be_enabled()
        expect(second.locator("p", has_text=answer["content"])).to_have_count(1)
        expect(second.locator("input[x-model=input]")).to_have_value("My next question")
        assert not errors, errors
        context.close()
        browser.close()
