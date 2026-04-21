import logging
from typing import Optional, Tuple

import requests

from config import (
    FUSEKI_DATASET_NAME,
    FUSEKI_PASSWORD,
    FUSEKI_URL,
    FUSEKI_USERNAME,
)

logger = logging.getLogger(__name__)


def fuseki_auth() -> Optional[Tuple[str, str]]:
    if FUSEKI_USERNAME and FUSEKI_PASSWORD:
        return (FUSEKI_USERNAME, FUSEKI_PASSWORD)
    return None


def _update_url() -> str:
    return f"{FUSEKI_URL}/{FUSEKI_DATASET_NAME}/update"


def _data_url() -> str:
    return f"{FUSEKI_URL}/{FUSEKI_DATASET_NAME}/data"


def query_url() -> str:
    return f"{FUSEKI_URL}/{FUSEKI_DATASET_NAME}/sparql"


def replace_subject_in_graph(
    graph_uri: str,
    subject_uri: str,
    triples_nt: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
) -> bool:
    """DELETE the subject + up to 3 levels of blank-node descendants in <graph_uri>,
    then INSERT the provided N-Triples in the same graph, in a single SPARQL Update.

    Returns True on success.
    """
    sparql = f"""WITH <{graph_uri}>
DELETE {{
  ?root ?p0 ?o0 .
  ?bn1 ?p1 ?o1 .
  ?bn2 ?p2 ?o2 .
  ?bn3 ?p3 ?o3 .
}}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?root ?p0 ?o0 .
  OPTIONAL {{
    ?root ?px0 ?bn1 .
    FILTER(isBlank(?bn1))
    ?bn1 ?p1 ?o1 .
    OPTIONAL {{
      ?bn1 ?px1 ?bn2 .
      FILTER(isBlank(?bn2))
      ?bn2 ?p2 ?o2 .
      OPTIONAL {{
        ?bn2 ?px2 ?bn3 .
        FILTER(isBlank(?bn3))
        ?bn3 ?p3 ?o3 .
      }}
    }}
  }}
}} ;
INSERT DATA {{
  GRAPH <{graph_uri}> {{
    {triples_nt}
  }}
}}
"""
    http = session or requests
    response = http.post(
        _update_url(),
        data=sparql,
        headers={"Content-Type": "application/sparql-update"},
        auth=fuseki_auth(),
        timeout=timeout,
    )
    if response.status_code not in (200, 204):
        logger.error(
            "SPARQL update failed for <%s> in <%s>: %s %s",
            subject_uri, graph_uri, response.status_code, response.text[:200],
        )
        return False
    return True


def upload_turtle(
    graph_uri: str,
    turtle: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
) -> bool:
    """POST Turtle to the /data endpoint, replacing the named graph contents."""
    http = session or requests
    response = http.post(
        _data_url(),
        params={"graph": graph_uri},
        data=turtle.encode("utf-8") if isinstance(turtle, str) else turtle,
        headers={"Content-Type": "text/turtle; charset=utf-8"},
        auth=fuseki_auth(),
        timeout=timeout,
    )
    if response.status_code not in (200, 201, 204):
        logger.error(
            "Turtle upload to <%s> failed: %s %s",
            graph_uri, response.status_code, response.text[:200],
        )
        return False
    return True


def sparql_select(query: str, *, session: Optional[requests.Session] = None, timeout: int = 30) -> list:
    """Run a SELECT query and return the bindings list (empty on error)."""
    http = session or requests
    try:
        response = http.get(
            query_url(),
            params={"query": query, "format": "application/sparql-results+json"},
            auth=fuseki_auth(),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()["results"]["bindings"]
    except Exception as e:
        logger.warning("SPARQL SELECT failed: %s", e)
        return []


def sparql_construct_jsonld(
    query: str, *, session: Optional[requests.Session] = None, timeout: int = 60
) -> Optional[dict]:
    """Run a CONSTRUCT query and return the JSON-LD body (None on error/empty)."""
    http = session or requests
    try:
        response = http.get(
            query_url(),
            params={"query": query, "format": "application/ld+json"},
            auth=fuseki_auth(),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning("SPARQL CONSTRUCT failed: %s", e)
        return None
