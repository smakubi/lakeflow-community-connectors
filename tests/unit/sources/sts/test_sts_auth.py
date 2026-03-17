import json
from pathlib import Path
from urllib import error, parse, request

import pytest

CONFIG_PATH = Path(__file__).parent / "configs" / "dev_config.json"
AUTH_FAILURE_CODES = {401, 403}


def _load_sts_config() -> dict:
    config = json.loads(CONFIG_PATH.read_text())
    required_keys = [
        "client_id",
        "client_secret",
        "token_url",
        "audience",
        "base_url",
    ]
    missing_keys = [key for key in required_keys if not config.get(key)]
    assert not missing_keys, f"Missing required STS config keys: {missing_keys}"
    return config


def _fetch_access_token(config: dict) -> str:
    payload = json.dumps(
        {
            "grant_type": "client_credentials",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "audience": config["audience"],
        }
    ).encode("utf-8")
    token_request = request.Request(
        config["token_url"],
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(token_request, timeout=20) as response:
            response_body = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        pytest.fail(
            f"STS token exchange failed with HTTP {exc.code}. "
            "Verify the OAuth client credentials, audience, and token URL."
        )
    except error.URLError as exc:
        pytest.fail(
            f"STS token exchange could not reach the token endpoint: {exc.reason}. "
            "Verify network access to the configured Auth0 token URL."
        )

    access_token = response_body.get("access_token")
    token_type = response_body.get("token_type", "")
    assert isinstance(access_token, str) and access_token, (
        "Token endpoint response did not include a non-empty access_token."
    )
    assert token_type.lower() == "bearer", (
        f"Expected bearer token_type, got {token_type!r}."
    )
    return access_token


@pytest.fixture(scope="module")
def sts_config() -> dict:
    return _load_sts_config()


@pytest.fixture(scope="module")
def sts_access_token(sts_config: dict) -> str:
    return _fetch_access_token(sts_config)


def test_sts_oauth_client_credentials_exchange(sts_access_token: str):
    assert sts_access_token


def test_sts_authenticated_base_url_probe(sts_config: dict, sts_access_token: str):
    base_url = sts_config["base_url"].rstrip("/") + "/"
    probe_request = request.Request(
        base_url,
        headers={
            "Authorization": f"Bearer {sts_access_token}",
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with request.urlopen(probe_request, timeout=20) as response:
            assert response.status < 500, (
                f"Authenticated probe returned unexpected server error HTTP {response.status}."
            )
    except error.HTTPError as exc:
        assert exc.code not in AUTH_FAILURE_CODES, (
            f"Authenticated probe was rejected with HTTP {exc.code}. "
            "Token acquisition worked, but the STS API did not accept the bearer token."
        )
        assert exc.code < 500, (
            f"Authenticated probe returned unexpected server error HTTP {exc.code}."
        )
    except error.URLError as exc:
        pytest.fail(
            f"Authenticated probe could not reach the STS base URL: {exc.reason}. "
            "Verify network access and the configured base_url."
        )
