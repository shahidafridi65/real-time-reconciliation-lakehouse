import importlib


def test_normalize_database_url_converts_postgres_scheme():
    settings = importlib.import_module('config.settings')

    normalized = settings.normalize_database_url('postgres://user:pass@localhost:5432/app')

    assert normalized == 'postgresql://user:pass@localhost:5432/app'


def test_validate_runtime_config_detects_missing_database_url(monkeypatch):
    settings = importlib.import_module('config.settings')

    monkeypatch.delenv('DATABASE_URL', raising=False)

    try:
        settings.validate_runtime_config()
        assert False, 'Expected ValueError for missing DATABASE_URL'
    except ValueError as exc:
        assert 'DATABASE_URL' in str(exc)
