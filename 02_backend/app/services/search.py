"""Read-only proxy to the Meilisearch search endpoint of the configured index.

Public callers (the course catalogue frontend) cannot reach Meilisearch directly:
it exposes no ports in production. This module forwards their search requests
using MEILISEARCH_SEARCH_KEY, a key that must only carry the `search` action.
The master key (MEILISEARCH_API_KEY, used by the gold stage) is deliberately
never used here, so a missing search key fails closed instead of exposing writes.
"""

import logging
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import HTTPException, status

from config import (
    MEILISEARCH_INDEX,
    MEILISEARCH_SEARCH_KEY,
    MEILISEARCH_TIMEOUT,
    MEILISEARCH_URL,
    SEARCH_MAX_LIMIT,
)

logger = logging.getLogger(__name__)

# Params that must not exceed SEARCH_MAX_LIMIT, whichever paging style is used.
_CAPPED_PARAMS = ("limit", "hitsPerPage")


def _search_url() -> str:
    return f"{MEILISEARCH_URL}/indexes/{MEILISEARCH_INDEX}/search"


def _search_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MEILISEARCH_SEARCH_KEY}",
    }


def _require_config() -> None:
    if not MEILISEARCH_SEARCH_KEY or not MEILISEARCH_INDEX:
        logger.error(
            "Search proxy unavailable: MEILISEARCH_SEARCH_KEY and MEILISEARCH_INDEX must both be set"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Search is not configured",
        )


def _cap(value: Any) -> Any:
    """Clamp a limit-ish value to SEARCH_MAX_LIMIT, leaving junk for Meilisearch to reject."""
    try:
        return min(int(value), SEARCH_MAX_LIMIT)
    except (TypeError, ValueError):
        return value


def _clamped(payload: Dict[str, Any]) -> Dict[str, Any]:
    capped = dict(payload)
    for key in _CAPPED_PARAMS:
        if key in capped:
            capped[key] = _cap(capped[key])
    return capped


def search(
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Any]:
    """Forward one search request and relay Meilisearch's status code and JSON body.

    Pass `body` for the POST form of the search API, `params` for the GET form.
    Errors from Meilisearch itself (e.g. a malformed `filter`) are handed back
    verbatim so callers see the real message rather than a generic 500.
    """
    _require_config()

    try:
        if params is not None:
            response = requests.get(
                _search_url(),
                headers=_search_headers(),
                params=_clamped(params),
                timeout=MEILISEARCH_TIMEOUT,
            )
        else:
            response = requests.post(
                _search_url(),
                headers=_search_headers(),
                json=_clamped(body or {}),
                timeout=MEILISEARCH_TIMEOUT,
            )
    except requests.Timeout:
        logger.warning("Search request to Meilisearch timed out")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Search backend timed out",
        )
    except requests.RequestException as e:
        logger.warning("Search request to Meilisearch failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Search backend unavailable",
        )

    try:
        return response.status_code, response.json()
    except ValueError:
        logger.warning(
            "Meilisearch returned a non-JSON response (status %s)", response.status_code
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Invalid response from search backend",
        )
