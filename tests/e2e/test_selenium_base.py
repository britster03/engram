"""Selenium base fixtures for mounted Admin UI E2E testing."""
import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def selenium_driver():
    """Chrome driver fixture."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    try:
        # First try with the local chromedriver
        driver = webdriver.Chrome(options=options)
    except OSError:
        try:
            # Fallback to webdriver_manager
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options,
            )
        except OSError:
            pytest.skip("Chrome/Selenium not properly configured")
            return
    
    driver.implicitly_wait(10)
    yield driver
    driver.quit()


@pytest.fixture(scope="function")
def admin_logged_in_driver(selenium_driver):
    """Return the browser; individual tests perform explicit Admin auth."""
    yield selenium_driver
