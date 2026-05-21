from typing import Any, Dict, Optional

import logging

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


def _resolve_credentials(
    db: Session, provider_uuid: str, auth: Dict[str, Any]
) -> Dict[str, Any]:
    """Resolve OAuth client credentials for a source.

    Manifest credentials win when present (manifest.process_manifest already
    enforces that both client_id and client_secret are set together). Otherwise
    look up the (provider_uuid, token_endpoint) row in provider_oauth_cred.
    """
    token_endpoint = auth.get("token_endpoint")
    if not token_endpoint:
        raise ValueError("oauth2.0 auth is missing token_endpoint")

    if auth.get("client_id") and auth.get("client_secret"):
        return {
            "client_id": auth["client_id"],
            "client_secret": auth["client_secret"],
            "scope": auth.get("scope"),
            "token_endpoint": token_endpoint,
        }

    row = db.execute(
        text("""
            SELECT client_id, client_secret, scope
            FROM provider_oauth_cred
            WHERE provider_uuid = :provider_uuid
              AND token_endpoint = :token_endpoint
        """),
        {"provider_uuid": str(provider_uuid), "token_endpoint": token_endpoint},
    ).fetchone()
    if not row:
        raise ValueError(
            f"No OAuth credentials configured for provider {provider_uuid} at {token_endpoint}"
        )
    return {
        "client_id": row[0],
        "client_secret": row[1],
        "scope": auth.get("scope") or row[2],
        "token_endpoint": token_endpoint,
    }


def get_oauth_token(
    db: Session, provider_uuid: str, auth: Dict[str, Any]
) -> str:
    """Run the client_credentials flow and return the access token string."""
    creds = _resolve_credentials(db, provider_uuid, auth)

    data = {
        "grant_type": "client_credentials",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    }
    if creds.get("scope"):
        data["scope"] = creds["scope"]

    resp = requests.post(
        creds["token_endpoint"],
        data=data,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    token: Optional[str] = payload.get("access_token")
    if not token:
        raise RuntimeError(
            f"Token endpoint {creds['token_endpoint']} returned no access_token"
        )
    return token
