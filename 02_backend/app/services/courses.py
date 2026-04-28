import json
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from fastapi import HTTPException, status
from pyld import jsonld
from rdflib.namespace import OWL, RDF, SKOS
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import (
    GRAPH_COURSES,
    GRAPH_REFERENCE,
    GRAPH_VOCABULARY,
    SCHEMA_DIR,
)
from services import fuseki

DCTERMS_NS = "http://purl.org/dc/terms/"
RDF_NS = str(RDF)
OWL_NS = str(OWL)
SKOS_NS = str(SKOS)

QL_NS = "http://data.quality-link.eu/ontology/v1#"
ELM_NS = "http://data.europa.eu/snb/model/elm/"

FRAME_JSON_PATH = SCHEMA_DIR / "frame.json"

class CourseNotFound(Exception):
    """
    A course (specified as UUID or URI) could not be found.
    """
    pass

@lru_cache(maxsize=1)
def _frame_config() -> dict:
    with open(FRAME_JSON_PATH, "r") as f:
        return json.load(f)

def resolve_course_uri(uuid: str) -> Optional[str]:
    """
    Look up course URI based on UUID
    """

    uri_query = f"""
PREFIX rdf: <{RDF}>
PREFIX ql: <{QL_NS}>
PREFIX elm: <{ELM_NS}>
PREFIX owl: <{OWL}>

SELECT ?learningOpportunity
FROM <{GRAPH_COURSES}>
WHERE {{
  VALUES ?type {{
    ql:LearningOpportunitySpecification
    elm:Qualification
    elm:LearningAchievementSpecification
  }}
  <urn:uuid:{uuid}> owl:sameAs ?learningOpportunity .
  ?learningOpportunity rdf:type ?type .
}}
"""
    bindings = fuseki.sparql_select(uri_query)

    if not bindings:
        raise CourseNotFound("Course UUID not found.")

    return bindings[0]["learningOpportunity"]["value"]


def resolve_course_uuid(uri: str) -> Optional[str]:

    uuid_query = f"""
PREFIX rdf: <{RDF}>
PREFIX ql: <{QL_NS}>
PREFIX elm: <{ELM_NS}>
PREFIX owl: <{OWL}>

SELECT ?uuid
FROM <{GRAPH_COURSES}>
WHERE {{
  VALUES ?type {{
    ql:LearningOpportunitySpecification
    elm:Qualification
    elm:LearningAchievementSpecification
  }}
  ?uuid owl:sameAs <{uri}> .
  <{uri}> rdf:type ?type .
}}
"""
    bindings = fuseki.sparql_select(uuid_query)

    if not bindings or not bindings[0]["uuid"]["value"].startswith("urn:uuid:"):
        raise CourseNotFound("Course UUID not found.")

    return bindings[0]["uuid"]["value"][len("urn:uuid:"):]


def frame_course(course_uri: str) -> Optional[Dict[str, Any]]:

    frame_config = _frame_config()
    uploaded = 0
    failed = 0

    construct_query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

CONSTRUCT {{ ?s ?p ?o . }}
FROM <{GRAPH_COURSES}>
FROM <{GRAPH_REFERENCE}>
FROM <{GRAPH_VOCABULARY}>
WHERE {{
  <{course_uri}> (<>|!<>)* ?s .
  ?s ?p ?o .
}}
"""

    raw_nt = fuseki.sparql_construct_nt(construct_query)
    if not raw_nt:
        raise CourseNotFound("SPARQL query returned no data.")

    return jsonld.frame(jsonld.from_rdf(raw_nt, options={"useNativeTypes":True}), frame_config)


def list_provider_courses(
    db: Session,
    provider_uuid: UUID,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """List courses published by a provider (queried from the Fuseki courses graph)."""
    row = db.execute(
        text("SELECT base_id FROM provider WHERE provider_uuid = :uuid"),
        {"uuid": provider_uuid},
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Provider not found",
        )
    if not row[0]:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Provider {provider_uuid} has no base_id; cannot resolve its RDF URI",
        )
    provider_uri = f"https://data.deqar.eu/institution/{row[0]}"

    count_query = f"""
PREFIX rdf: <{RDF_NS}>
PREFIX ql:  <{QL_NS}>
PREFIX elm: <{ELM_NS}>
PREFIX dcterms: <{DCTERMS_NS}>

SELECT (COUNT(DISTINCT ?los) AS ?n)
FROM <{GRAPH_COURSES}>
WHERE {{
  VALUES ?t {{ ql:LearningOpportunitySpecification elm:Qualification elm:LearningAchievementSpecification }}
  ?los rdf:type ?t ;
       dcterms:publisher <{provider_uri}> .
}}
"""
    count_bindings = fuseki.sparql_select(count_query)
    total = int(count_bindings[0]["n"]["value"]) if count_bindings else 0

    list_query = f"""
PREFIX rdf: <{RDF_NS}>
PREFIX owl: <{OWL_NS}>
PREFIX dcterms: <{DCTERMS_NS}>
PREFIX skos: <{SKOS_NS}>
PREFIX ql:  <{QL_NS}>
PREFIX elm: <{ELM_NS}>

SELECT ?course_uuid ?los (SAMPLE(?typeLabel) AS ?type) (SAMPLE(?anyTitle) AS ?title) (COUNT(?loi) AS ?instances)
FROM <{GRAPH_COURSES}>
FROM <{GRAPH_VOCABULARY}>
WHERE {{
  VALUES ?class {{ ql:LearningOpportunitySpecification elm:Qualification elm:LearningAchievementSpecification }}
  ?los rdf:type ?class ;
       dcterms:publisher <{provider_uri}> .
  ?uuid_node owl:sameAs ?los .
  FILTER(STRSTARTS(STR(?uuid_node), "urn:uuid:"))
  BIND(STRAFTER(STR(?uuid_node), "urn:uuid:") AS ?course_uuid)
  OPTIONAL {{ ?los dcterms:title ?anyTitle . }}
  OPTIONAL {{ ?los dcterms:type ?typeConcept . ?typeConcept skos:prefLabel ?typeLabel . }}
  OPTIONAL {{ ?los elm:learningOpportunity ?loi . }}
}}
GROUP BY ?course_uuid ?los
ORDER BY ?course_uuid
LIMIT {int(limit)} OFFSET {int(offset)}
"""
    bindings = fuseki.sparql_select(list_query)

    courses = []
    for b in bindings:
        type_iri = b.get("type", {}).get("value", "") or ""
        type_label = type_iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1] or None
        title_binding = b.get("title") or {}
        courses.append({
            "course_uuid": b.get("course_uuid", {}).get("value"),
            "uri": b.get("los", {}).get("value"),
            "instances": b.get("instances", {}).get("value"),
            "type": type_label,
            "type_uri": type_iri or None,
            "title": title_binding.get("value"),
            "title_lang": title_binding.get("xml:lang"),
        })

    return {
        "response": courses,
        "total": total,
        "provider_uri": provider_uri,
        "limit": limit,
        "offset": offset,
    }

