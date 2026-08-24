"""Small shared helpers for secret-conscious metadata handling"""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

_SENSITIVE_FIELD_FRAGMENTS = (
    "access-key",
    "access_key",
    "api-key",
    "api_key",
    "apikey",
    "authorization",
    "connection-string",
    "connection_string",
    "cookie",
    "credential",
    "database-url",
    "database_url",
    "dsn",
    "passphrase",
    "password",
    "private-key",
    "private_key",
    "secret",
    "signature",
    "token",
)
_SENSITIVE_URI_QUERY_FRAGMENTS = (
    "access_key",
    "accesskey",
    "api-key",
    "api_key",
    "apikey",
    "credential",
    "password",
    "secret",
    "signature",
    "token",
)
_SENSITIVE_URI_QUERY_KEYS = {"sig"}


def is_sensitive_field_name(name: str) -> bool:
    """Return whether a structured-log field name strongly suggests secret material"""

    lowered = name.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_FIELD_FRAGMENTS)


def _contains_sensitive_pairs(component: str) -> bool:
    # ``;`` is still used as a query separator by some systems, while modern
    # ``parse_qsl`` intentionally recognizes only ``&``. Treat both as separators
    # so a SAS/token parameter cannot bypass redaction through the legacy form.
    for key, _value in parse_qsl(component.replace(";", "&"), keep_blank_values=True):
        lowered = key.lower()
        if lowered in _SENSITIVE_URI_QUERY_KEYS:
            return True
        if any(fragment in lowered for fragment in _SENSITIVE_URI_QUERY_FRAGMENTS):
            return True
    return False


def contains_sensitive_uri_data(value: str) -> bool:
    """Detect common credential-bearing URI forms without logging the value itself"""

    try:
        parsed = urlsplit(value)
    except ValueError:
        return True
    if parsed.username is not None or parsed.password is not None:
        return True
    return _contains_sensitive_pairs(parsed.query) or _contains_sensitive_pairs(parsed.fragment)
