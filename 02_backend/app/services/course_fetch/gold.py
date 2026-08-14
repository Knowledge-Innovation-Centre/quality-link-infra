import logging
from typing import Dict, List, Optional

import requests

from config import (
    GRAPH_COURSES,
    MEILISEARCH_API_KEY,
    MEILISEARCH_INDEX,
    MEILISEARCH_URL,
)
from services import fuseki
from services.course_fetch.ratio import resolve_student_staff_ratio
from services.courses import (
    CourseNotFound,
    frame_course,
    resolve_course_uri,
)

logger = logging.getLogger(__name__)


def _meili_url() -> str:
    return f"{MEILISEARCH_URL}/indexes/{MEILISEARCH_INDEX}/documents"


def _meili_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if MEILISEARCH_API_KEY:
        headers["Authorization"] = f"Bearer {MEILISEARCH_API_KEY}"
    return headers


def reindex_course(
    session: requests.Session,
    course_uuid: str,
    course_uri: Optional[str] = None,
) -> bool:
    """Frame one course and upsert it into Meilisearch.

    If `course_uri` is None, resolve it from the UUID via Fuseki. Callers that
    already have the URI (e.g. the silver→gold handoff, `list_provider_courses`,
    `list_all_courses`) should pass it to skip the resolve roundtrip.
    """
    if course_uri is None:
        try:
            course_uri = resolve_course_uri(course_uuid)
        except CourseNotFound as e:
            logger.warning("Resolve failed for %s: %s", course_uuid, e)
            return False

    # framed["id"] is read inside the try: frame_course guarantees a single
    # top-level node, but a malformed frame would otherwise raise KeyError here
    # and escape all the way out of index_gold, failing the whole run.
    try:
        framed = frame_course(course_uri)
        framed.pop("@context", None)
        framed["uri"] = framed["id"]
        framed["id"] = course_uuid
    except Exception as e:
        logger.warning("Framing failed for %s: %s", course_uuid, e)
        return False

    if "elm:learningOpportunity" in framed and isinstance(framed["elm:learningOpportunity"], list):
        count = len(framed["elm:learningOpportunity"])
        if count > 0:
            framed["instanceCount"] = len(framed["elm:learningOpportunity"])

    try:
        ratio = resolve_student_staff_ratio(framed, session=session)
        if ratio is not None:
            framed["studentStaffRatio"] = ratio
    except Exception as e:
        # Enrichment must never cost a course its index entry.
        logger.warning("Student-staff ratio lookup failed for %s: %s", course_uuid, e)

    try:
        r = session.post(_meili_url(), headers=_meili_headers(), json=framed, timeout=30)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.warning("Meilisearch upload failed for %s: %s", course_uuid, e)
        return False


def list_all_courses() -> List[Dict[str, str]]:
    """Enumerate every course in the Fuseki courses graph as {uuid, uri} pairs."""
    query = f"""
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ql:  <http://data.quality-link.eu/ontology/v1#>
PREFIX elm: <http://data.europa.eu/snb/model/elm/>

SELECT DISTINCT ?uuid_node ?los
FROM <{GRAPH_COURSES}>
WHERE {{
  VALUES ?t {{
    ql:LearningOpportunitySpecification
    elm:Qualification
    elm:LearningAchievementSpecification
  }}
  {{
    ?uuid_node owl:sameAs ?los .
    ?los rdf:type ?t .
    FILTER(STRSTARTS(STR(?uuid_node), "urn:uuid:"))
  }} UNION {{
    ?los rdf:type ?t .
    FILTER(STRSTARTS(STR(?los), "urn:uuid:"))
  }}
}}
"""
    bindings = fuseki.sparql_select(query)
    courses: List[Dict[str, str]] = []
    for b in bindings:
        uri = b.get("los", {}).get("value")
        if not uri:
            continue
        if uri.startswith("urn:uuid:"):
            courses.append({"uuid": uri[len("urn:uuid:"):], "uri": uri})
        else:
            uuid_node = b.get("uuid_node", {}).get("value", "")
            if not uuid_node.startswith("urn:uuid:"):
                continue
            courses.append({"uuid": uuid_node[len("urn:uuid:"):], "uri": uri})
    return courses


def index_gold(
    session: requests.Session, courses: List[Dict[str, str]]
) -> tuple[int, int]:
    """Upsert each course into Meilisearch. Returns (uploaded, failed)."""
    if not courses:
        logger.info("Gold: nothing to index")
        return 0, 0

    uploaded = 0
    failed = 0
    for c in courses:
        if reindex_course(session, c["uuid"], c.get("uri")):
            uploaded += 1
        else:
            failed += 1

    logger.info("Gold: uploaded=%s failed=%s", uploaded, failed)
    return uploaded, failed
