import logging
from dataclasses import dataclass
from typing import List

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, SKOS

from config import GRAPH_VOCABULARY
from services import fuseki

logger = logging.getLogger(__name__)

EU_SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"


@dataclass
class VocabStats:
    scheme_uri: str
    concepts: int = 0
    bytes_uploaded: int = 0
    success: bool = False
    error: str = ""


def fetch_skos_concepts(scheme_uri: str, *, timeout: int = 60) -> List[dict]:
    query = f"""PREFIX skos: <{SKOS}>

SELECT DISTINCT ?concept_uri ?label_en
FROM <{scheme_uri}>
WHERE {{
  ?concept_uri a skos:Concept .
  ?concept_uri skos:prefLabel ?label_en .
  FILTER(lang(?label_en) = "en")
}}
"""
    response = requests.get(
        EU_SPARQL_ENDPOINT,
        params={
            "query": query,
            "format": "application/sparql-results+json",
            "timeout": "0",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    bindings = response.json().get("results", {}).get("bindings", [])

    concepts = []
    for b in bindings:
        concept_uri = (b.get("concept_uri") or {}).get("value")
        label_en = (b.get("label_en") or {}).get("value")
        if concept_uri and label_en:
            concepts.append({"concept_uri": concept_uri, "label_en": label_en})

    logger.info("Vocabulary %s: fetched %s concepts", scheme_uri, len(concepts))
    return concepts


def build_skos_turtle(concepts: List[dict], scheme_uri: str) -> str:
    scheme = URIRef(scheme_uri)
    graph = Graph()
    graph.bind("skos", SKOS)
    graph.bind("rdf", RDF)

    for item in concepts:
        concept = URIRef(item["concept_uri"])
        graph.add((concept, RDF.type, SKOS.Concept))
        graph.add((concept, SKOS.prefLabel, Literal(item["label_en"], lang="en")))
        graph.add((concept, SKOS.inScheme, scheme))

    return graph.serialize(format="turtle")


def refresh_vocabulary(scheme_uri: str) -> VocabStats:
    stats = VocabStats(scheme_uri=scheme_uri)
    try:
        concepts = fetch_skos_concepts(scheme_uri)
        stats.concepts = len(concepts)
        if not concepts:
            stats.error = "no concepts returned"
            return stats
        turtle = build_skos_turtle(concepts, scheme_uri)
        stats.bytes_uploaded = len(turtle)
        stats.success = fuseki.upload_turtle(GRAPH_VOCABULARY, turtle)
        if not stats.success:
            stats.error = "fuseki upload failed"
    except Exception as e:
        logger.exception("refresh_vocabulary failed for %s", scheme_uri)
        stats.error = str(e)
    return stats
