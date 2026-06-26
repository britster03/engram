# tests/e2e/conftest.py
import os

import pytest

pytestmark = pytest.mark.e2e

BASE_URL = "http://localhost:8000"


def _load_env_key(key_name):
    """Load a key from .env file."""
    key = os.environ.get(key_name)
    if key:
        return key
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    try:
        with open(env_path) as f:
            for line in f:
                if line.startswith(f"{key_name}="):
                    key = line.strip().split("=", 1)[1]
                    break
    except FileNotFoundError:
        pass
    return key


@pytest.fixture(scope="session")
def base_url():
    return os.environ.get("ENGRAM_BASE_URL", BASE_URL)


@pytest.fixture(scope="session")
def admin_key():
    return _load_env_key("ENGRAM_ADMIN_KEY")


@pytest.fixture(scope="session")
def api_key():
    return _load_env_key("ENGRAM_API_KEY")
