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


def sparql_update(
    update: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
    context: str = "",
) -> bool:
    """POST a SPARQL Update to the /update endpoint. Returns True on success.

    `context` is only used to make the log line identifiable on failure.
    """
    http = session or requests
    try:
        response = http.post(
            _update_url(),
            data=update.encode("utf-8"),
            headers={"Content-Type": "application/sparql-update"},
            auth=fuseki_auth(),
            timeout=timeout,
        )
    except requests.exceptions.RequestException as e:
        logger.error("SPARQL update failed %s: %s", context, e)
        return False
    if response.status_code not in (200, 204):
        logger.error(
            "SPARQL update failed %s: %s %s",
            context, response.status_code, response.text[:200],
        )
        return False
    return True


def _subject_delete_blocks(
    graph_uri: str,
    subject_uri: str,
    *,
    delete_alias: bool,
) -> list:
    """The SPARQL Update statements removing one subject from <graph_uri>.

    Covers the subject itself, each elm:learningOpportunity instance it points to,
    and up to 3 levels of blank-node descendants of either. Returned as separate
    statements so callers can join them with ` ;` and optionally append their own
    (see `replace_subject_in_graph`).

    Deepest blank nodes first, and roots bound through `VALUES ?root` rather than a
    property path over the whole graph — both deliberate for query performance.
    """
    blocks = [
        f"""# depth 3 — deepest blank nodes first
WITH <{graph_uri}>
DELETE {{ ?b3 ?p ?o }}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?root elm:learningOpportunity? ?a .
  ?a  ?q1 ?b1 . FILTER(isBlank(?b1))
  ?b1 ?q2 ?b2 . FILTER(isBlank(?b2))
  ?b2 ?q3 ?b3 . FILTER(isBlank(?b3))
  ?b3 ?p ?o .
}}""",
        f"""# depth 2
WITH <{graph_uri}>
DELETE {{ ?b2 ?p ?o }}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?root elm:learningOpportunity? ?a .
  ?a  ?q1 ?b1 . FILTER(isBlank(?b1))
  ?b1 ?q2 ?b2 . FILTER(isBlank(?b2))
  ?b2 ?p ?o .
}}""",
        f"""# depth 1
WITH <{graph_uri}>
DELETE {{ ?b1 ?p ?o }}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?root elm:learningOpportunity? ?a .
  ?a ?q1 ?b1 . FILTER(isBlank(?b1))
  ?b1 ?p ?o .
}}""",
        f"""# the anchors themselves: root + its learning opportunities
WITH <{graph_uri}>
DELETE {{ ?a ?p ?o }}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?root elm:learningOpportunity? ?a .
  ?a ?p ?o .
}}""",
    ]

    if delete_alias:
        blocks.append(f"""# delete alias
WITH <{graph_uri}>
DELETE {{ ?alias owl:sameAs ?root . }}
WHERE {{
  VALUES ?root {{ <{subject_uri}> }}
  ?alias owl:sameAs ?root .
}}""")

    return blocks


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

    blocks = _subject_delete_blocks(
        graph_uri, subject_uri, delete_alias=bool(alias_replace)
    )
    blocks.append(f"""# insert new data
INSERT DATA {{
  GRAPH <{graph_uri}> {{
    {triples_nt}
    {alias_nt}
  }}
}}""")

    sparql = f"""
PREFIX owl: <{OWL}>
PREFIX elm: <{ELM}>

""" + " ;\n".join(blocks) + "\n"

    return sparql_update(
        sparql,
        session=session,
        timeout=timeout,
        context=f"for <{subject_uri}> in <{graph_uri}>",
    )


def delete_subject_in_graph(
    graph_uri: str,
    subject_uri: str,
    *,
    delete_alias: bool = True,
    session: Optional[requests.Session] = None,
    timeout: int = 60,
) -> bool:
    """Remove one subject from <graph_uri> without inserting a replacement.

    Same cascade as `replace_subject_in_graph` minus the INSERT: the subject, its
    elm:learningOpportunity instances, their blank-node descendants, and (unless
    `delete_alias=False`) any `<alias> owl:sameAs <subject>` triple. Dropping the
    alias matters — otherwise `resolve_course_uri`/`resolve_course_uuid` keep
    resolving a course whose triples are gone.

    Returns True on success.
    """
    sparql = f"""
PREFIX owl: <{OWL}>
PREFIX elm: <{ELM}>

""" + " ;\n".join(
        _subject_delete_blocks(graph_uri, subject_uri, delete_alias=delete_alias)
    ) + "\n"

    return sparql_update(
        sparql,
        session=session,
        timeout=timeout,
        context=f"deleting <{subject_uri}> from <{graph_uri}>",
    )


def drop_graph(
    graph_uri: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 300,
) -> bool:
    """DROP an entire named graph. Returns True on success."""
    return sparql_update(
        f"DROP GRAPH <{graph_uri}>",
        session=session,
        timeout=timeout,
        context=f"dropping <{graph_uri}>",
    )


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
