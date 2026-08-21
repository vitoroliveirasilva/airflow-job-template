from scripts.check_secrets import scan


def test_secret_scan_rejects_real_env_file(tmp_path) -> None:
    (tmp_path / ".env").write_text("TOKEN=placeholder\n", encoding="utf-8")
    assert scan(tmp_path) == ["forbidden file: .env"]


def test_secret_scan_detects_high_confidence_key_without_literal_in_test_source(
    tmp_path,
) -> None:
    key = "AK" + "IA" + ("A" * 16)
    (tmp_path / "settings.txt").write_text(f"access={key}\n", encoding="utf-8")
    assert scan(tmp_path) == ["AWS access key: settings.txt"]
