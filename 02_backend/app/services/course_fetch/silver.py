import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from minio import Minio
from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, XSD
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import MINIO_BUCKET_NAME, GRAPH_COURSES, GRAPH_REFERENCE
from services import fuseki

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")

DEFAULT_TYPE = URIRef("http://data.europa.eu/snb/learning-opportunity/05053c1cbe")

def _has_type(graph: Graph, subject, *types) -> bool:
    return any((subject, RDF.type, t) in graph for t in types)


def _collect(src: Graph, dst: Graph, node, visited: set) -> None:
    if node in visited:
        return
    visited.add(node)
    for p, o in src.predicate_objects(node):
        dst.add((node, p, o))
        if isinstance(o, BNode):
            _collect(src, dst, o, visited)


def _extract_subgraph(graph: Graph, root: URIRef) -> Graph:
    sub = Graph()
    _collect(graph, sub, root, set())
    return sub


def _fetch_same_as_map(session: requests.Session) -> Dict[str, str]:
    query = f"""
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
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
) -> Tuple[List[str], Optional[Graph]]:
    """Parse, enrich in place, return (course_uuids, graph)."""
    import uuid as uuid_lib

    try:
        graph = Graph()
        graph.parse(data=file_content, format=file_format)
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)
        graph.bind("owl", OWL)

        now = datetime.now(timezone.utc)
        today = now.date()

        course_uuids: set[str] = set()
        owl_same_as: list[tuple] = []
        loi_subjects: list[URIRef] = []
        los_subjects: list[URIRef] = []

        for subject in graph.subjects(unique=True):
            if not isinstance(subject, URIRef):
                continue
            graph.add((subject, QL.ingestedDate, Literal(today, datatype=XSD.date)))
            graph.add((subject, QL.ingestedAt, Literal(now, datatype=XSD.dateTime)))

            if _has_type(graph, subject, QL.HigherEducationInstitution, ELM.Organisation):
                continue

            if _has_type(graph, subject, QL.LearningOpportunitySpecification,
                         ELM.Qualification, ELM.LearningAchievementSpecification):
                course_uuid = str(uuid_lib.uuid5(uuid_lib.UUID(provider_uuid), str(subject)))
                owl_same_as.append((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, subject))
                los_subjects.append(subject)
                course_uuids.add(course_uuid)

            elif _has_type(graph, subject, QL.LearningOpportunityInstance, ELM.LearningOpportunity):
                loi_subjects.append(subject)

        for triple in owl_same_as:
            graph.add(triple)

        for loi in loi_subjects:
            if (loi, ELM.providedBy, None) not in graph and provider_uri:
                graph.add((loi, ELM.providedBy, URIRef(provider_uri)))

        for los_uri in los_subjects:
            # set default values
            if (los_uri, QL.isActive, None) not in graph:
                graph.add((los_uri, QL.isActive, Literal(True)))

            if (los_uri, DCTERMS.type, None) not in graph:
                graph.add((los_uri, DCTERMS.type, DEFAULT_TYPE))

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

        logger.info(
            "Enriched: %s LOS, %s LOI, %s course_uuids, %s triples",
            len(los_subjects), len(loi_subjects), len(course_uuids), len(graph),
        )
        return list(course_uuids), graph

    except Exception as e:
        logger.exception("RDF enrichment failed: %s", e)
        return [], None


def enrich_silver(
    db: Session,
    minio_client: Minio,
    session: requests.Session,
    message: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Download bronze file, enrich, push each subject to Fuseki, update source row.

    Returns {provider_uuid, source_version_uuid, source_type, course_uuids} or None.
    """
    provider_uuid = message["provider_uuid"]
    source_version_uuid = message["source_version_uuid"]
    source_uuid = message["source_uuid"]
    source_type = message.get("source_type", "unknown")
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

    course_uuids, enriched_graph = _enrich_rdf_graph(
        file_content, file_format, provider_uuid, provider_uri, same_as_map
    )
    if enriched_graph is None:
        return None

    named_uris = [s for s in enriched_graph.subjects(unique=True) if isinstance(s, URIRef)]
    failed = 0
    for uri in named_uris:
        subgraph_nt = _extract_subgraph(enriched_graph, uri).serialize(format="nt")
        ok = fuseki.replace_subject_in_graph(
            GRAPH_COURSES, str(uri), subgraph_nt, session=session
        )
        if not ok:
            failed += 1
    logger.info("Pushed %s/%s subjects to Fuseki courses graph", len(named_uris) - failed, len(named_uris))

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

    return {
        "provider_uuid": provider_uuid,
        "source_version_uuid": source_version_uuid,
        "source_type": source_type,
        "course_uuids": course_uuids,
    }
