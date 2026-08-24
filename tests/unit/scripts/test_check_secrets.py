import pytest
from scripts.check_secrets import scan


def test_secret_scan_rejects_real_env_file(tmp_path) -> None:
    (tmp_path / ".env").write_text("TOKEN=placeholder\n", encoding="utf-8")
    assert scan(tmp_path) == ["forbidden env file: .env"]


def test_secret_scan_rejects_env_variants_but_allows_example(tmp_path) -> None:
    (tmp_path / ".env.production").write_text("PASSWORD=placeholder\n", encoding="utf-8")
    (tmp_path / ".env.example").write_text("TOKEN=\n", encoding="utf-8")
    assert scan(tmp_path) == ["forbidden env file: .env.production"]


def test_secret_scan_detects_high_confidence_key_without_literal_in_test_source(tmp_path) -> None:
    key = "AK" + "IA" + ("A" * 16)
    (tmp_path / "settings.txt").write_text(f"access={key}\n", encoding="utf-8")
    assert scan(tmp_path) == ["AWS access key: settings.txt"]


def test_secret_scan_detects_credential_bearing_uri(tmp_path) -> None:
    uri = "postgresql://service:" + "password" + "@db.invalid/app"
    (tmp_path / "settings.txt").write_text(f"DATABASE_URL={uri}\n", encoding="utf-8")
    assert scan(tmp_path) == ["credential URI: settings.txt"]


def test_secret_scan_rejects_missing_root(tmp_path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(ValueError, match="not a directory"):
        scan(missing)


def test_secret_scan_checks_extensionless_and_pem_text_files(tmp_path) -> None:
    private_key_marker = "-----BEGIN " + "PRIVATE KEY-----"
    (tmp_path / "Dockerfile").write_text(private_key_marker, encoding="utf-8")
    (tmp_path / "server.pem").write_text(private_key_marker, encoding="utf-8")

    assert scan(tmp_path) == [
        "private key: Dockerfile",
        "private key: server.pem",
    ]


def test_secret_scan_skips_binary_files(tmp_path) -> None:
    key = "AK" + "IA" + ("A" * 16)
    (tmp_path / "artifact.bin").write_bytes(b"\x00" + key.encode("ascii"))
    assert scan(tmp_path) == []


@pytest.mark.parametrize(
    ("label", "token"),
    [
        ("GitLab token", "gl" + "pat-" + "A" * 24),
        ("Google API key", "AI" + "za" + "A" * 35),
        ("npm token", "npm" + "_" + "A" * 36),
        ("PyPI token", "pypi-" + "AgEIcHlwaS5vcmc" + "A" * 24),
        ("Stripe live secret", "sk" + "_live_" + "A" * 24),
    ],
)
def test_secret_scan_detects_additional_high_confidence_tokens(
    tmp_path, label: str, token: str
) -> None:
    (tmp_path / "settings.txt").write_text(f"value={token}\n", encoding="utf-8")
    assert scan(tmp_path) == [f"{label}: settings.txt"]


def test_secret_scan_skips_generated_local_temp_tree(tmp_path) -> None:
    generated = tmp_path / ".tmp" / "pytest"
    generated.mkdir(parents=True)
    (generated / ".env").write_text("TOKEN=test-fixture\n", encoding="utf-8")
    assert scan(tmp_path) == []
