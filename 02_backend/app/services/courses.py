import json
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from fastapi import HTTPException, status
from pyld import jsonld
from rdflib.namespace import (
    OWL,
    RDF,
    RDFS,
    SKOS,
)
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
ADMS_NS = "http://www.w3.org/ns/adms#"
FOAF_NS = "http://xmlns.com/foaf/0.1/"

FRAME_JSON_PATH = SCHEMA_DIR / "frame.json"

# Predicates whose object is a reference to another learning opportunity, not a
# description of one. Emit the link, never traverse into the neighbour —
# otherwise the closure from one course reaches its programme, then every
# sibling course, then all their offerings.
# Keep in sync with LOS_LINK_PREDICATES in course_fetch/silver.py.
LOS_LINK_PREDICATES = (
    "elm:isPartOf",
    "elm:hasPart",
    "elm:specialisationOf",
    "elm:generalisationOf",
    "ql:superseded",
    "ql:entryRequirementLearningOpportunity",
    "ql:limitLearningOpportunity",
)

# The subset the frontend renders as a navigable link, and therefore the only
# ones a display stub is fetched for. The stub mirrors the profile's
# ql:LearningOpportunitySpecificationReference — enough to label a link without
# dereferencing it. dcterms:type is deliberately absent; add a branch for it and
# its skos:prefLabel if a link ever needs badging as programme-vs-course.
# Keep in sync with the matching sub-frames in schema/frame.json — JSON has no
# refs, so they are duplicated there.
LOS_DISPLAY_LINKS = (
    "elm:isPartOf",
    "elm:hasPart",
    "elm:specialisationOf",
    "elm:generalisationOf",
)

# Other fan-out edges the frame never reads. foaf:member drags every other
# member of the publisher's alliance into the document — measured on one real
# course, blocking it alone cut the constructed graph from 882 triples to 237.
# The skos:* entries cost nothing on today's vocabularies but stop one
# vocabulary load with skos:hasTopConcept from pulling in every concept.
CLOSURE_BLOCKED = LOS_LINK_PREDICATES + (
    "foaf:member",
    "skos:broader",
    "skos:narrower",
    "skos:related",
    "skos:inScheme",
)

# Pre-rendered for the SPARQL below: a negated property set and a VALUES list.
_BLOCKED_PATH = "|".join(CLOSURE_BLOCKED)
_DISPLAY_LINKS = " ".join(LOS_DISPLAY_LINKS)


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

    # Pin the root. The frame's top-level filter matches on @type alone, and a
    # neighbour LOS is the same class — without an "id" pyld matches it too,
    # hoists both nodes into @graph, and the relation frames as null.
    # _frame_config() is lru_cached and shared, so copy rather than mutate.
    frame_config = {**_frame_config(), "id": course_uri}

    # Bounded closure. `!(...)` is a negated property set, so traversal skips
    # the blocked predicates while still emitting them (the zero-length case
    # includes the root itself, so the root's own link triples are returned by
    # the first branch). The remaining branches read a display stub for each
    # directly-linked neighbour straight from the store, which is why silver
    # does not copy one into the child's subgraph.
    #
    # ql:uuid is synthesised here and is NOT stored in Fuseki: the alias runs
    # the other way (`urn:uuid:X owl:sameAs <neighbour>`) and JSON-LD framing
    # cannot follow an inverse property.
    #
    # Separate UNIONs, not sibling OPTIONALs: siblings would multiply into a
    # |title| x |identifier| cross-product per neighbour for identical output.
    construct_query = f"""
PREFIX rdf: <{RDF}>
PREFIX rdfs: <{RDFS}>
PREFIX owl: <{OWL}>
PREFIX skos: <{SKOS}>
PREFIX dcterms: <{DCTERMS_NS}>
PREFIX adms: <{ADMS_NS}>
PREFIX foaf: <{FOAF_NS}>
PREFIX elm: <{ELM_NS}>
PREFIX ql: <{QL_NS}>

CONSTRUCT {{
  ?s ?p ?o .
  ?nb ql:uuid ?nbUuid .
  ?nb dcterms:title ?nbTitle .
  ?nb adms:identifier ?nbId .
  ?nbId ?nbIdP ?nbIdO .
}}
FROM <{GRAPH_COURSES}>
FROM <{GRAPH_REFERENCE}>
FROM <{GRAPH_VOCABULARY}>
WHERE {{
  {{
    <{course_uri}> (!({_BLOCKED_PATH}))* ?s .
    ?s ?p ?o .
  }} UNION {{
    VALUES ?rel {{ {_DISPLAY_LINKS} }}
    <{course_uri}> ?rel ?nb .
    ?nbNode owl:sameAs ?nb .
    FILTER(STRSTARTS(STR(?nbNode), "urn:uuid:"))
    BIND(STRAFTER(STR(?nbNode), "urn:uuid:") AS ?nbUuid)
  }} UNION {{
    VALUES ?rel {{ {_DISPLAY_LINKS} }}
    <{course_uri}> ?rel ?nb .
    ?nb dcterms:title ?nbTitle .
  }} UNION {{
    VALUES ?rel {{ {_DISPLAY_LINKS} }}
    <{course_uri}> ?rel ?nb .
    ?nb adms:identifier ?nbId .
    ?nbId ?nbIdP ?nbIdO .
  }}
}}
"""

    raw_nt = fuseki.sparql_construct_nt(construct_query)
    if not raw_nt:
        raise CourseNotFound("SPARQL query returned no data.")

    framed = jsonld.frame(
        jsonld.from_rdf(raw_nt, options={"useNativeTypes": True}), frame_config
    )

    # Guard the hoisting failure mode above: turn it into a skipped document
    # rather than a KeyError escaping into the caller's run.
    if "@graph" in framed or framed.get("id") != course_uri:
        raise CourseNotFound(
            f"Framing did not yield a single node for {course_uri}"
        )

    return framed


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

SELECT ?course_uuid ?los (SAMPLE(?typeLabel) AS ?type) (SAMPLE(?anyTitle) AS ?title) (COUNT(DISTINCT ?loi) AS ?instances)
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

