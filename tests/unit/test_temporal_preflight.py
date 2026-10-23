from scripts.temporal_preflight import _valid_postgres_url


def test_postgres_url_requires_credentials_host_and_database():
    assert _valid_postgres_url("postgresql://engram_app:secret@postgres.internal:5432/engram")
    assert not _valid_postgres_url("mysql://user:secret@database.internal/engram")
    assert not _valid_postgres_url("postgresql://postgres.internal/engram")
    assert not _valid_postgres_url("postgresql://user:secret@postgres.internal")
