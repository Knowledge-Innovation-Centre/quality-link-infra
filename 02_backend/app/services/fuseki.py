import logging
from typing import Optional, Tuple

import requests

from rdflib.namespace import OWL

from config import (
    FUSEKI_DATASET_NAME,
    FUSEKI_PASSWORD,
    FUSEKI_URL,
    FUSEKI_USERNAME,
)

ELM = "http://data.europa.eu/snb/model/elm/"

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
    alias_uri: Optional[str] = None,
    alias_replace: Optional[bool] = False,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
) -> bool:
    """DELETE the subject + up to 3 levels of blank-node descendants in <graph_uri>,
    plus each elm:learningOpportunity instance the subject points to and its own
    3 levels of blank-node descendants; then INSERT the provided N-Triples in the
    same graph, in a single SPARQL Update.

    Returns True on success.
    """

    if alias_uri and alias_uri != subject_uri:
        alias_nt = f"<{alias_uri}> owl:sameAs <{subject_uri}> ."
    else:
        alias_nt = ""

    if alias_replace:
        alias_delete = "?alias owl:sameAs ?root ."
        alias_where = "OPTIONAL { ?alias owl:sameAs ?root . }"
    else:
        alias_delete = ""
        alias_where = ""

    sparql = f"""
PREFIX owl: <{OWL}>
PREFIX elm: <{ELM}>

WITH <{graph_uri}>
DELETE {{
  ?root ?p0 ?o0 .
  ?bn1 ?p1 ?o1 .
  ?bn2 ?p2 ?o2 .
  ?bn3 ?p3 ?o3 .
  ?inst ?ip0 ?io0 .
  ?ibn1 ?ip1 ?io1 .
  ?ibn2 ?ip2 ?io2 .
  ?ibn3 ?ip3 ?io3 .
  {alias_delete}
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
  OPTIONAL {{
    ?root elm:learningOpportunity ?inst .
    ?inst ?ip0 ?io0 .
    OPTIONAL {{
      ?inst ?ipx0 ?ibn1 .
      FILTER(isBlank(?ibn1))
      ?ibn1 ?ip1 ?io1 .
      OPTIONAL {{
        ?ibn1 ?ipx1 ?ibn2 .
        FILTER(isBlank(?ibn2))
        ?ibn2 ?ip2 ?io2 .
        OPTIONAL {{
          ?ibn2 ?ipx2 ?ibn3 .
          FILTER(isBlank(?ibn3))
          ?ibn3 ?ip3 ?io3 .
        }}
      }}
    }}
  }}
  {alias_where}
}} ;
INSERT DATA {{
  GRAPH <{graph_uri}> {{
    {triples_nt}
    {alias_nt}
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
    """POST Turtle to the /data endpoint, adding to the named graph contents."""
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
    """Run a SPARQL SELECT query and return the bindings list (empty on error)."""
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
        logger.warning("SPARQL query failed: %s", e)
        return []


def sparql_construct_jsonld(
    query: str, *, session: Optional[requests.Session] = None, timeout: int = 60
) -> Optional[dict]:
    """Run a SPARQL query and return the JSON-LD body (None on error/empty)."""
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
        logger.warning("SPARQL query failed: %s", e)
        return None


def sparql_construct_nt(
    query: str, *, session: Optional[requests.Session] = None, timeout: int = 60
) -> Optional[dict]:
    """Run a SPARQL query and return as N-Triples (None on error/empty)."""
    http = session or requests
    try:
        response = http.get(
            query_url(),
            params={"query": query, "format": "application/n-triples"},
            auth=fuseki_auth(),
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.warning("SPARQL query failed: %s", e)
        return None
