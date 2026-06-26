"""Selenium E2E tests for the Engram admin UI.

These tests exercise the full browser stack: login, navigation,
ingest pipeline, chat, and SQLite data propagation.
"""

import os
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC  # noqa: N812
from selenium.webdriver.support.ui import WebDriverWait

pytestmark = pytest.mark.e2e

TAB_LABELS = {
    "status": "Status",
    "sessions": "Sessions",
    "memories": "Memories",
    "ingest": "Ingest",
    "settings": "Settings",
}


def _event_ledger_path() -> Path:
    override = os.environ.get("ENGRAM_EVENT_LEDGER_PATH")
    if override:
        return Path(override).expanduser().resolve()

    cfg_path = os.environ.get("ENGRAM_CONFIG_PATH")
    if cfg_path:
        with open(cfg_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        configured = (cfg.get("event_ledger") or {}).get("path")
        if configured:
            path = Path(configured).expanduser()
            return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()

    return (Path.cwd() / "data" / "event_ledger.db").resolve()


@pytest.fixture(scope="session")
def selenium_driver():
    """Chrome driver fixture — reuses existing chromedriver setup."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    try:
        driver = webdriver.Chrome(options=options)
    except OSError:
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options,
        )

    driver.implicitly_wait(5)
    yield driver
    driver.quit()


@pytest.fixture(scope="function")
def admin_logged_in_driver(selenium_driver, base_url, admin_key):
    """Return a driver that is already logged into the admin dashboard."""
    drv = selenium_driver
    drv.delete_all_cookies()
    drv.get(f"{base_url}/admin/login")
    drv.execute_script("localStorage.clear();")

    wait = WebDriverWait(drv, 30)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]')))

    pwd_input = drv.find_element(By.CSS_SELECTOR, 'input[type="password"]')
    pwd_input.clear()
    pwd_input.send_keys(admin_key)

    submit_btn = drv.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
    submit_btn.click()

    wait.until(EC.url_contains("/admin/dashboard"))
    wait.until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, 'nav img[src*="engram_logo"]')
        )
    )

    yield drv

    drv.delete_all_cookies()
    drv.get(f"{base_url}/admin/login")
    drv.execute_script("localStorage.clear();")


def _nav_tab_button(wait: WebDriverWait, tab_key: str):
    label = TAB_LABELS[tab_key]
    return wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//nav//button[contains(normalize-space(.), '{label}')]")
        )
    )


def _set_ingest_session(drv, session_id: str) -> None:
    session_input = drv.find_element(By.CSS_SELECTOR, 'input[x-model="ingest.session_id"]')
    session_input.clear()
    session_input.send_keys(session_id)


@pytest.mark.e2e
def test_login_page_shows_engram_logo(selenium_driver, base_url):
    """Login page must display the Engram logo (not old omlx logos)."""
    drv = selenium_driver
    drv.get(f"{base_url}/admin/login")

    wait = WebDriverWait(drv, 30)
    logo = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'img[src*="engram_logo"]'))
    )
    assert logo.is_displayed()
    src = logo.get_attribute("src")
    assert "engram_logo" in src
    assert "omlx" not in src.lower()


@pytest.mark.e2e
def test_login_redirects_to_dashboard(selenium_driver, base_url, admin_key):
    """Valid admin key logs the user in and redirects to /admin/dashboard."""
    drv = selenium_driver
    drv.get(f"{base_url}/admin/login")

    wait = WebDriverWait(drv, 30)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]')))

    pwd_input = drv.find_element(By.CSS_SELECTOR, 'input[type="password"]')
    pwd_input.clear()
    pwd_input.send_keys(admin_key)

    submit_btn = drv.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
    submit_btn.click()

    wait.until(EC.url_contains("/admin/dashboard"))
    assert "/admin/dashboard" in drv.current_url


@pytest.mark.e2e
def test_dashboard_tabs_exist(admin_logged_in_driver):
    """The dashboard must show all 5 tab buttons: Status, Sessions, Memories, Ingest, Settings."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)

    tabs = {
        "Status": "status",
        "Sessions": "sessions",
        "Memories": "memories",
        "Ingest": "ingest",
        "Settings": "settings",
    }

    for label, tab_key in tabs.items():
        btn = wait.until(
            EC.visibility_of_element_located(
                (By.XPATH, f"//nav//button[contains(., '{label}')]")
            )
        )
        assert btn.is_displayed()
        assert label in btn.text
        click_attr = btn.get_attribute("@click")
        assert f"setMainTab('{tab_key}')" in (click_attr or "")


@pytest.mark.e2e
def test_dashboard_tab_navigation(admin_logged_in_driver, base_url):
    """Clicking each tab button must reveal its corresponding panel."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)

    tab_checks = {
        "status": "//h3[contains(text(), 'System Status')]",
        "sessions": "//h3[contains(text(), 'Active Sessions')]",
        "memories": "//h3[contains(text(), 'Memories')]",
        "ingest": "//h3[contains(text(), 'Ingest Pipeline')]",
        "settings": "//h3[contains(text(), 'Settings')]",
    }

    for tab_key, heading_xpath in tab_checks.items():
        btn = _nav_tab_button(wait, tab_key)
        drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        btn.click()

        wait.until(EC.visibility_of_element_located((By.XPATH, heading_xpath)))


@pytest.mark.e2e
def test_chat_page_loads(selenium_driver, base_url, admin_key):
    """/admin/chat must load and allow sending a message."""
    drv = selenium_driver

    drv.get(f"{base_url}/admin/login")
    wait = WebDriverWait(drv, 30)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="password"]')))
    drv.find_element(By.CSS_SELECTOR, 'input[type="password"]').send_keys(admin_key)
    drv.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()
    wait.until(EC.url_contains("/admin/dashboard"))

    drv.get(f"{base_url}/admin/chat")
    wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, '#chat-history')))

    initial_count = drv.execute_script("return document.querySelectorAll('#chat-history > div').length")

    inp = drv.find_element(By.CSS_SELECTOR, 'input[type="text"]')
    inp.clear()
    inp.send_keys("What is the weather today?")

    send_btn = drv.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", send_btn)
    send_btn.click()

    wait.until(
        lambda d: d.execute_script(
            "return document.querySelectorAll('#chat-history > div').length"
        )
        > initial_count,
        message="Chat messages did not grow after sending",
    )


@pytest.mark.e2e
def test_ingest_via_dashboard(admin_logged_in_driver, base_url):
    """Ingest tab: fill user + assistant fields, submit, and observe success."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)

    ingest_btn = _nav_tab_button(wait, "ingest")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", ingest_btn)
    ingest_btn.click()

    wait.until(EC.visibility_of_element_located((By.XPATH, "//h3[contains(text(), 'Ingest Pipeline')]")))

    user_ta = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'textarea[x-model="ingest.user"]')))
    asst_ta = drv.find_element(By.CSS_SELECTOR, 'textarea[x-model="ingest.assistant"]')
    _set_ingest_session(drv, f"e2e-dashboard-{uuid4().hex}")

    user_ta.clear()
    user_ta.send_keys("E2E test user message")
    asst_ta.clear()
    asst_ta.send_keys("E2E test assistant response")

    submit = drv.find_element(By.CSS_SELECTOR, "button[type='submit']")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit)
    submit.click()

    wait.until(
        EC.visibility_of_element_located((By.XPATH, "//span[contains(text(), 'Ingested:')]")),
        message="Ingest success indicator did not appear",
    )


@pytest.mark.e2e
def test_session_appears_after_ingest(admin_logged_in_driver, base_url):
    """After ingest, the Sessions tab should list at least one session."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)

    ingest_btn = _nav_tab_button(wait, "ingest")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", ingest_btn)
    ingest_btn.click()

    wait.until(EC.visibility_of_element_located((By.XPATH, "//h3[contains(text(), 'Ingest Pipeline')]")))

    user_ta = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'textarea[x-model="ingest.user"]')))
    asst_ta = drv.find_element(By.CSS_SELECTOR, 'textarea[x-model="ingest.assistant"]')
    _set_ingest_session(drv, f"e2e-session-{uuid4().hex}")
    user_ta.clear()
    user_ta.send_keys("Session test query")
    asst_ta.clear()
    asst_ta.send_keys("Session test answer")

    submit = drv.find_element(By.CSS_SELECTOR, "button[type='submit']")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit)
    submit.click()

    wait.until(
        EC.visibility_of_element_located((By.XPATH, "//span[contains(text(), 'Ingested:')]")),
        message="Ingest success indicator did not appear after session ingest",
    )

    drv.get(f"{base_url}/admin/dashboard?tab=sessions")
    wait.until(EC.visibility_of_element_located((By.XPATH, "//h3[contains(text(), 'Active Sessions')]")))

    try:
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//table//tbody//tr")),
            message="No session rows appeared after ingest",
        )
    except Exception:
        sessions_len = drv.execute_script(
            "var el = document.querySelector('[x-data=\"dashboard()\"]'); "
            "return el ? el.__x.$data.sessions.length : -1;"
        )
        assert sessions_len is not None and sessions_len > 0, "No sessions found after ingest"


@pytest.mark.e2e
def test_sqlite_events_after_ingest(admin_logged_in_driver, base_url):
    """After ingest, the SQLite events table must contain a new row."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)

    db_path = _event_ledger_path()
    conn_before = sqlite3.connect(str(db_path))
    cur_before = conn_before.cursor()
    cur_before.execute("SELECT COUNT(*) FROM events")
    count_before = cur_before.fetchone()[0]
    conn_before.close()

    ingest_btn = _nav_tab_button(wait, "ingest")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", ingest_btn)
    ingest_btn.click()
    wait.until(EC.visibility_of_element_located((By.XPATH, "//h3[contains(text(), 'Ingest Pipeline')]")))

    user_ta = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'textarea[x-model="ingest.user"]')))
    asst_ta = drv.find_element(By.CSS_SELECTOR, 'textarea[x-model="ingest.assistant"]')
    _set_ingest_session(drv, f"e2e-sqlite-{uuid4().hex}")
    user_ta.clear()
    user_ta.send_keys("SQLite propagation test")
    asst_ta.clear()
    asst_ta.send_keys("SQLite propagation response")

    submit = drv.find_element(By.CSS_SELECTOR, "button[type='submit']")
    drv.execute_script("arguments[0].scrollIntoView({block: 'center'});", submit)
    submit.click()

    wait.until(
        EC.visibility_of_element_located((By.XPATH, "//span[contains(text(), 'Ingested:')]")),
        message="Ingest success indicator did not appear",
    )

    conn_after = sqlite3.connect(str(db_path))
    cur_after = conn_after.cursor()
    cur_after.execute("SELECT COUNT(*) FROM events")
    count_after = cur_after.fetchone()[0]
    conn_after.close()

    assert count_after > count_before, (
        f"Expected events table to grow, but before={count_before} and after={count_after}"
    )


@pytest.mark.e2e
def test_navbar_shows_engram_logo(admin_logged_in_driver):
    """The dashboard navbar must display the Engram logo."""
    drv = admin_logged_in_driver
    wait = WebDriverWait(drv, 30)
    logo = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'nav img[src*="engram_logo"]'))
    )
    assert logo.is_displayed()
    assert "engram_logo" in logo.get_attribute("src")
