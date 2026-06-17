from stock_data_service.config import Settings


def test_settings_from_env_loads_dotenv_without_overriding_existing_env(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "STOCK_DATA_DATA_ROOT=./from-file",
                "BAIDU_APP_KEY=file-key",
                "BAIDU_APP_SECRET=file-secret",
                "BAIDU_TOKEN_FILE=./file-token.json",
                "BAIDU_REDIRECT_URI=http://localhost:8000/admin/api/baidu/oauth/callback",
                "BAIDU_SCOPE=basic,netdisk",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("STOCK_DATA_DATA_ROOT", raising=False)
    monkeypatch.delenv("BAIDU_APP_KEY", raising=False)
    monkeypatch.delenv("BAIDU_TOKEN_FILE", raising=False)
    monkeypatch.delenv("BAIDU_REDIRECT_URI", raising=False)
    monkeypatch.delenv("BAIDU_SCOPE", raising=False)
    monkeypatch.setenv("BAIDU_APP_SECRET", "env-secret")

    settings = Settings.from_env()

    assert settings.data_root.as_posix() == "from-file"
    assert settings.baidu_app_key == "file-key"
    assert settings.baidu_app_secret == "env-secret"
    assert settings.baidu_token_file.as_posix() == "file-token.json"
    assert settings.baidu_redirect_uri == "http://localhost:8000/admin/api/baidu/oauth/callback"
    assert settings.baidu_scope == "basic,netdisk"
