import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid

import requests
from minio import Minio
from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, XSD
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import MINIO_BUCKET_NAME, GRAPH_COURSES, GRAPH_REFERENCE
from services import fuseki
from services.course_fetch import skilldata

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")

DEFAULT_TYPE = URIRef("http://data.europa.eu/snb/learning-opportunity/05053c1cbe")
PROGRAMME_TYPE = URIRef("http://data.europa.eu/snb/learning-opportunity/79343569f3")

# Predicates whose object is a *reference* to another learning opportunity, not
# a description of one. The link triple is kept, but the referenced node is
# never traversed into: it is a separate root with its own push, so copying any
# of its triples here would insert statements that
# fuseki.replace_subject_in_graph's root-scoped DELETE never removes (and whose
# blank nodes INSERT DATA would relabel, accumulating a duplicate subtree per
# run).
#
# This is every sh:path in reference/qualitylink-profile.ttl whose range is
# ql:LearningOpportunity{Specification,Instance}Reference, plus
# elm:generalisationOf — absent from the QL profile but present in ELM LOQ, so
# it can still arrive via an ELM source.
LOS_LINK_PREDICATES = frozenset({
    ELM.isPartOf,
    ELM.hasPart,
    ELM.specialisationOf,
    ELM.generalisationOf,
    QL.superseded,
    QL.entryRequirementLearningOpportunity,
    QL.limitLearningOpportunity,
})


def _has_type(graph: Graph, subject, *types) -> bool:
    """
    Checks if subject has any of a given list of RDF types (classes)
    """
    return any((subject, RDF.type, t) in graph for t in types)


def _is_uuid(uri):
    """
    Check if the URI is a urn:uuid one
    """
    UUID_PREFIX = "urn:uuid:"

    if str(uri).strip().startswith(UUID_PREFIX):
        try:
            return uuid.UUID(str(uri).strip()[len(UUID_PREFIX):])
        except ValueError:
            return False
    else:
        return False


def _collect(src: Graph, dst: Graph, node, visited: set, stop_nodes: frozenset) -> None:
    """
    Recursively collect all statements starting from node,
    avoiding loops by tracking visited nodes.

    Traversal stops at references to other learning opportunities: the link
    triple is kept, but the referenced node is not described. Both stop
    conditions are needed and cover different cases:

    - `LOS_LINK_PREDICATES` catches a neighbour described inline without a
      class this pipeline recognises. It is also the only condition that
      closes ql:limitLearningOpportunity, whose range is a LOS reference *or*
      a LOI reference — a LOI is not in `stop_nodes`, so without the predicate
      check the walk would enter the other course's offering, follow
      elm:learningAchievementSpecification up to that course, and continue
      into all of its offerings. The check fires at every depth, which matters
      because the property sits several blank nodes below a LOI.
    - `stop_nodes` catches the converse: a LOS reached by some predicate not on
      the list at all, including any path added to the profile later.
    """
    if node in visited:
        return
    visited.add(node)
    for p, o in src.predicate_objects(node):
        dst.add((node, p, o))
        if not isinstance(o, (BNode, URIRef)):
            continue
        if p in LOS_LINK_PREDICATES or o in stop_nodes:
            continue
        _collect(src, dst, o, visited, stop_nodes)


def _extract_subgraph(graph: Graph, root: URIRef, stop_nodes=frozenset()) -> Graph:
    """
    Extract a sub-graph starting from root, stopping at references to other
    learning opportunities.

    `stop_nodes` is the set of sibling LOS roots in the same graph. `root`
    itself is always removed from it — otherwise a root that is its own
    stop-node would yield an empty subgraph.
    """
    sub = Graph()
    _collect(graph, sub, root, set(), frozenset(stop_nodes) - {root})
    return sub


def _truncated_isced_f(code, length: int):
    """Truncate an elm:ISCEDFCode value to `length` digits, preserving its kind.

    `code` is the ISCED-F detailed-field code as it appears in the graph: either
    a URIRef ending in the digits (e.g. .../isced-f/0613) or a digit Literal.
    Returns a value of the same kind with the code truncated (URI prefix kept),
    or None when it is not an all-digit code of at least `length` digits.
    """
    if isinstance(code, URIRef):
        prefix, _, tail = str(code).rpartition("/")
        if tail.isdigit() and len(tail) >= length:
            return URIRef(f"{prefix}/{tail[:length]}")
    elif isinstance(code, Literal):
        tail = str(code).strip()
        if tail.isdigit() and len(tail) >= length:
            return Literal(tail[:length])
    return None


def _fetch_same_as_map(session: requests.Session) -> Dict[str, str]:
    query = f"""
PREFIX owl: <{OWL}>
PREFIX rdf: <{RDF}>
PREFIX ql:  <{QL}>
PREFIX elm: <{ELM}>

SELECT ?uriA ?uriB
FROM <{GRAPH_REFERENCE}>
WHERE {{
  ?uriA owl:sameAs ?uriB .
  ?uriB rdf:type ?type .
  VALUES ?type {{ ql:HigherEducationInstitution elm:Organisation }}
}}
"""
    bindings = fuseki.sparql_select(query, session=session)
    return {b["uriA"]["value"]: b["uriB"]["value"] for b in bindings}


def _enrich_rdf_graph(
    file_content: bytes, file_format: str,
    provider_uuid: str, provider_uri: Optional[str],
    same_as_map: Dict[str, str],
    session: requests.Session,
) -> Tuple[List[Dict[str, str]], Optional[Graph]]:
    """Parse, enrich in place, return (courses, graph) where each course is a
    {"uuid": str, "uri": str} dict."""

    try:
        graph = Graph()
        graph.parse(data=file_content, format=file_format)
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)
        graph.bind("owl", OWL)

        now = datetime.now(timezone.utc)
        today = now.date()

        courses: Dict[str, str] = {}
        owl_same_as: list[tuple] = []
        loi_subjects: list[URIRef] = []
        los_subjects: list[URIRef] = []

        # first iteration: identify LOS and LOIs in graph, create missing UUIDs
        for subject in graph.subjects(unique=True):
            if not isinstance(subject, URIRef):
                continue

            if _has_type(graph, subject, QL.LearningOpportunitySpecification,
                         ELM.Qualification, ELM.LearningAchievementSpecification):

                los_subjects.append(subject)

                # add metadata
                graph.add((subject, QL.ingestedDate, Literal(today, datatype=XSD.date)))
                graph.add((subject, QL.ingestedAt, Literal(now, datatype=XSD.dateTime)))

                # determined course UUID
                course_uuid = None
                if course_uuid := _is_uuid(subject):
                    # URI is a urn:uuid: one
                    pass
                else:
                    # check if UUID already in graph
                    for uuid_node in graph.subjects(OWL.sameAs, subject):
                        if course_uuid := _is_uuid(uuid_node):
                            break
                    if not course_uuid:
                        # generate a UUID if it does not already exist
                        course_uuid = uuid.uuid5(uuid.NAMESPACE_URL, str(subject))
                        owl_same_as.append((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, subject))
                courses[str(course_uuid)] = str(subject)

            elif _has_type(graph, subject, QL.LearningOpportunityInstance, ELM.LearningOpportunity):
                loi_subjects.append(subject)

        for triple in owl_same_as:
            graph.add(triple)

        for loi in loi_subjects:
            if (loi, ELM.providedBy, None) not in graph and provider_uri:
                graph.add((loi, ELM.providedBy, URIRef(provider_uri)))
            if same_as_map:
                for prov in list(graph.objects(loi, ELM.providedBy)):
                    if isinstance(prov, URIRef) and str(prov) in same_as_map:
                        graph.remove((loi, ELM.providedBy, prov))
                        graph.add((loi, ELM.providedBy, URIRef(same_as_map[str(prov)])))

        for los_uri in los_subjects:
            # set default values
            if (los_uri, QL.isActive, None) not in graph:
                graph.add((los_uri, QL.isActive, Literal(True)))

            if (los_uri, DCTERMS.type, None) not in graph:
                graph.add((los_uri, DCTERMS.type, DEFAULT_TYPE))

            if (los_uri, QL.sourceType, None) not in graph:
                graph.add((los_uri, QL.sourceType, QL.ELMSource))

            # convert ECTS credits to xsd:double
            if (los_uri, ELM.creditPoint, None) in graph:
                for creditPoint in graph.objects(los_uri, ELM.creditPoint):
                    for point in graph.objects(creditPoint, ELM.point):
                        if isinstance(point, Literal):
                            if point.datatype != XSD.double:
                                graph.remove((creditPoint, ELM.point, point))
                                try:
                                    newpoint = Literal(float(point), datatype=XSD.double)
                                    graph.add((creditPoint, ELM.point, newpoint))
                                except ValueError:
                                    logger.warning(f"{los_uri} has an invalid credit point value: {point}")
                        else:
                            logger.warning(f"{los_uri} has a credit point value that is not a Literal, cannot convert.")

            # resolve provider aliases to canonical URI
            if same_as_map:
                for pub in list(graph.objects(los_uri, DCTERMS.publisher)):
                    if isinstance(pub, URIRef) and str(pub) in same_as_map:
                        graph.remove((los_uri, DCTERMS.publisher, pub))
                        graph.add((los_uri, DCTERMS.publisher, URIRef(same_as_map[str(pub)])))

            # infer publisher from instances if unset
            if (los_uri, DCTERMS.publisher, None) not in graph:
                loi_providers = set()
                for loi in graph.subjects(ELM.learningAchievementSpecification, los_uri):
                    for p in graph.objects(loi, ELM.providedBy):
                        loi_providers.add(p)
                if loi_providers:
                    for p in loi_providers:
                        canonical = URIRef(same_as_map[str(p)]) if same_as_map and str(p) in same_as_map else p
                        graph.add((los_uri, DCTERMS.publisher, canonical))
                elif provider_uri:
                    graph.add((los_uri, DCTERMS.publisher, URIRef(provider_uri)))

            # create statements from LOS -> LOI
            for loi in graph.subjects(ELM.learningAchievementSpecification, los_uri):
                graph.add((los_uri, ELM.learningOpportunity, loi))

        if skilldata.is_configured():
            for los_uri in los_subjects:
                skilldata.enrich_course_with_skilldata(graph, los_uri, session=session)
        else:
            logger.info("skilldata: disabled (SKILLDATA_API_URL not set)")

        # derive ISCED-F broad (2-digit) and narrow (3-digit) fields from
        # elm:ISCEDFCode — after skilldata, which may have populated it
        for los_uri in los_subjects:
            for code in graph.objects(los_uri, ELM.ISCEDFCode):
                if (broad := _truncated_isced_f(code, 2)) is not None:
                    graph.add((los_uri, QL.ISCEDFBroadField, broad))
                if (narrow := _truncated_isced_f(code, 3)) is not None:
                    graph.add((los_uri, QL.ISCEDFNarrowField, narrow))

        programme_count = sum(
            1 for los_uri in los_subjects
            if (los_uri, DCTERMS.type, PROGRAMME_TYPE) in graph
        )
        logger.info(
            "Enriched: %s LOS (%s programmes), %s LOI, %s courses, %s triples",
            len(los_subjects), programme_count, len(loi_subjects), len(courses), len(graph),
        )
        return [{"uuid": u, "uri": uri} for u, uri in courses.items()], graph

    except Exception as e:
        logger.exception("RDF enrichment failed: %s", e)
        return [], None


def enrich_silver(
    db: Session,
    minio_client: Minio,
    session: requests.Session,
    message: Dict[str, Any],
) -> Optional[Tuple[List[Dict[str, str]], int]]:
    """Download bronze file, enrich, push each subject to Fuseki, update source row.

    Returns (uploaded_courses, total_count): a list of {"uuid", "uri"} dicts
    for the courses that were successfully pushed to Fuseki (so gold skips the
    ones that failed) and the total number of courses enriched. Returns None on
    failure (bronze download or RDF enrichment).
    """
    provider_uuid = message["provider_uuid"]
    source_uuid = message["source_uuid"]
    file_path = message["file_path"]
    file_format = message.get("file_format", "turtle")

    try:
        response = minio_client.get_object(MINIO_BUCKET_NAME, file_path)
        try:
            file_content = response.read()
        finally:
            response.close()
            response.release_conn()
    except Exception as e:
        logger.error("Failed to download bronze file %s: %s", file_path, e)
        return None

    provider_uri: Optional[str] = None
    row = db.execute(
        text("SELECT base_id FROM provider WHERE provider_uuid = :uuid"),
        {"uuid": provider_uuid},
    ).fetchone()
    if row and row[0]:
        provider_uri = f"https://data.deqar.eu/institution/{row[0]}"

    same_as_map = _fetch_same_as_map(session)
    logger.info("Loaded %s owl:sameAs mappings", len(same_as_map))

    courses, enriched_graph = _enrich_rdf_graph(
        file_content, file_format, provider_uuid, provider_uri, same_as_map, session
    )
    if enriched_graph is None:
        return None

    # Every LOS is pushed as its own root, so each one is a stop node for the
    # others: a subgraph describes exactly one learning opportunity and merely
    # links to its neighbours. That is what keeps each push proportional to its
    # own subject, and it is also why replace_subject_in_graph needs no change —
    # every triple here has the root or one of the root's own blank nodes as its
    # subject, which is precisely what its DELETE clause covers.
    stop_nodes = frozenset(URIRef(c["uri"]) for c in courses)

    uploaded: List[Dict[str, str]] = []
    for course in courses:
        subgraph_nt = _extract_subgraph(
            enriched_graph, URIRef(course['uri']), stop_nodes=stop_nodes
        ).serialize(format="nt")
        if fuseki.replace_subject_in_graph(GRAPH_COURSES, course['uri'], subgraph_nt, session=session, alias_uri=f"urn:uuid:{course['uuid']}", alias_replace=True):
            uploaded.append(course)
    logger.info("Pushed %s/%s LOS subjects to Fuseki courses graph", len(uploaded), len(courses))

    filename = os.path.basename(file_path)
    now = datetime.now(timezone.utc)
    db.execute(
        text("""
            UPDATE source
            SET last_file_pushed = :filename,
                last_file_pushed_date = :ts,
                last_file_pushed_path = :path,
                updated_at = :ts
            WHERE source_uuid = :source_uuid
        """),
        {"filename": filename, "ts": now, "path": file_path, "source_uuid": source_uuid},
    )
    db.commit()

    return uploaded, len(courses)
