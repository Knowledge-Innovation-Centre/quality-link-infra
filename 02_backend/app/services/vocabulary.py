import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Tuple, Union

import requests
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, SKOS
from rdflib.term import Identifier

from config import GRAPH_VOCABULARY
from services import fuseki

logger = logging.getLogger(__name__)

EU_SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"

EU_LANGUAGE_SCHEME = "http://publications.europa.eu/resource/authority/language"

_EUVOC_NS = "http://publications.europa.eu/ontology/euvoc#"
_ISO_639_DATATYPES = (
    f"{_EUVOC_NS}ISO_639_1",
    f"{_EUVOC_NS}ISO_639_2B",
    f"{_EUVOC_NS}ISO_639_2T",
    f"{_EUVOC_NS}ISO_639_3",
)

_LANGUAGE_INDEX: Optional[Dict[str, URIRef]] = None

VocabSpec = Union[str, Mapping[str, object]]


@dataclass
class VocabStats:
    scheme_uri: str
    concepts: int = 0
    extra_triples: int = 0
    bytes_uploaded: int = 0
    success: bool = False
    error: str = ""


def _normalize_spec(spec: VocabSpec) -> Tuple[str, List[str]]:
    """Accept a bare scheme URI or a {"scheme": ..., "properties": [...]} dict."""
    if isinstance(spec, str):
        return spec, []
    scheme = spec.get("scheme")
    if not scheme or not isinstance(scheme, str):
        raise ValueError(f"vocabulary spec missing 'scheme': {spec!r}")
    props = spec.get("properties") or []
    if not isinstance(props, (list, tuple)):
        raise ValueError(f"vocabulary 'properties' must be a list: {spec!r}")
    return scheme, [str(p) for p in props]


def _run_select(query: str, *, timeout: int) -> list:
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
    return response.json().get("results", {}).get("bindings", [])


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
    bindings = _run_select(query, timeout=timeout)

    concepts = []
    for b in bindings:
        concept_uri = (b.get("concept_uri") or {}).get("value")
        label_en = (b.get("label_en") or {}).get("value")
        if concept_uri and label_en:
            concepts.append({"concept_uri": concept_uri, "label_en": label_en})

    logger.info("Vocabulary %s: fetched %s concepts", scheme_uri, len(concepts))
    return concepts


def fetch_extra_triples(
    scheme_uri: str,
    properties: Iterable[str],
    *,
    timeout: int = 60,
) -> List[Tuple[str, str, Identifier]]:
    """Fetch (concept_uri, property_uri, value) for the requested extra properties.

    Literal values are restricted to English or untagged (incl. typed) literals.
    Blank-node values are skipped — they can't be re-emitted in a fresh graph
    without losing identity, and SKOS vocabularies very rarely use them anyway.
    """
    props = [p for p in properties if p]
    if not props:
        return []

    values_clause = " ".join(f"<{p}>" for p in props)
    query = f"""PREFIX skos: <{SKOS}>

SELECT DISTINCT ?concept_uri ?p ?o
FROM <{scheme_uri}>
WHERE {{
  VALUES ?p {{ {values_clause} }}
  ?concept_uri a skos:Concept ;
               ?p ?o .
  FILTER(!isLiteral(?o) || lang(?o) = "" || langMatches(lang(?o), "en"))
}}
"""
    bindings = _run_select(query, timeout=timeout)

    triples: List[Tuple[str, str, Identifier]] = []
    skipped_bnodes = 0
    for b in bindings:
        s = (b.get("concept_uri") or {}).get("value")
        p = (b.get("p") or {}).get("value")
        o = b.get("o") or {}
        if not s or not p or "value" not in o:
            continue
        node = _to_rdflib_node(o)
        if node is None:
            skipped_bnodes += 1
            continue
        triples.append((s, p, node))

    logger.info(
        "Vocabulary %s: fetched %s extra triple(s) across %s propert(y/ies)%s",
        scheme_uri,
        len(triples),
        len(props),
        f"; skipped {skipped_bnodes} bnode value(s)" if skipped_bnodes else "",
    )
    return triples


def _to_rdflib_node(b: dict) -> Optional[Identifier]:
    t = b.get("type")
    v = b.get("value", "")
    if t == "uri":
        return URIRef(v)
    if t == "bnode":
        return None
    # literal | typed-literal
    lang = b.get("xml:lang")
    dt = b.get("datatype")
    if lang:
        return Literal(v, lang=lang)
    if dt:
        return Literal(v, datatype=URIRef(dt))
    return Literal(v)


def build_skos_turtle(
    concepts: List[dict],
    scheme_uri: str,
    extras: Optional[List[Tuple[str, str, Identifier]]] = None,
) -> str:
    scheme = URIRef(scheme_uri)
    graph = Graph()
    graph.bind("skos", SKOS)
    graph.bind("rdf", RDF)

    known_concepts = set()
    for item in concepts:
        concept = URIRef(item["concept_uri"])
        graph.add((concept, RDF.type, SKOS.Concept))
        graph.add((concept, SKOS.prefLabel, Literal(item["label_en"], lang="en")))
        graph.add((concept, SKOS.inScheme, scheme))
        known_concepts.add(item["concept_uri"])

    for s, p, o in extras or []:
        if s not in known_concepts:
            # Extras query may return concepts without an English prefLabel; emit
            # the concept stub so the triple still resolves in Fuseki.
            graph.add((URIRef(s), RDF.type, SKOS.Concept))
            graph.add((URIRef(s), SKOS.inScheme, scheme))
            known_concepts.add(s)
        graph.add((URIRef(s), URIRef(p), o))

    return graph.serialize(format="turtle")


def _load_language_index() -> Dict[str, URIRef]:
    """Query the local vocabulary graph for ISO 639 notations on language concepts."""
    datatype_list = ", ".join(f"<{dt}>" for dt in _ISO_639_DATATYPES)
    query = f"""PREFIX skos: <{SKOS}>

SELECT ?concept ?notation
FROM <{GRAPH_VOCABULARY}>
WHERE {{
  ?concept skos:inScheme <{EU_LANGUAGE_SCHEME}> ;
           skos:notation ?notation .
  FILTER(datatype(?notation) IN ({datatype_list}))
}}
"""
    bindings = fuseki.sparql_select(query)
    index: Dict[str, URIRef] = {}
    for b in bindings:
        concept = (b.get("concept") or {}).get("value")
        notation = (b.get("notation") or {}).get("value")
        if concept and notation:
            index[notation.strip().lower()] = URIRef(concept)
    logger.info("Language index loaded: %s notation(s)", len(index))
    return index


def _clear_language_cache() -> None:
    global _LANGUAGE_INDEX
    _LANGUAGE_INDEX = None


def language_tag_to_uri(tag: str) -> Optional[URIRef]:
    """Look up the EU authority URI for a BCP 47 language tag.

    Only the primary language subtag is matched (e.g. ``"en-US" → "en"``);
    region / script / variant subtags are ignored. Matches case-insensitively
    against ``skos:notation`` values typed as ``euvoc:ISO_639_1``,
    ``ISO_639_2B``, ``ISO_639_2T`` or ``ISO_639_3`` in the local vocabulary
    graph. Returns ``None`` (and logs a warning) if no match is found.
    """
    if not isinstance(tag, str):
        return None
    primary = tag.strip().replace("_", "-").split("-", 1)[0].lower()
    if not primary:
        return None

    global _LANGUAGE_INDEX
    if _LANGUAGE_INDEX is None:
        _LANGUAGE_INDEX = _load_language_index()

    uri = _LANGUAGE_INDEX.get(primary)
    if uri is None:
        logger.warning("No EU language URI for tag %r (primary subtag %r)", tag, primary)
    return uri


def refresh_vocabulary(vocab: VocabSpec) -> VocabStats:
    scheme_uri, extra_properties = _normalize_spec(vocab)
    stats = VocabStats(scheme_uri=scheme_uri)
    try:
        concepts = fetch_skos_concepts(scheme_uri)
        stats.concepts = len(concepts)
        extras = fetch_extra_triples(scheme_uri, extra_properties) if extra_properties else []
        stats.extra_triples = len(extras)
        if not concepts and not extras:
            stats.error = "no concepts returned"
            return stats
        turtle = build_skos_turtle(concepts, scheme_uri, extras=extras)
        stats.bytes_uploaded = len(turtle)
        stats.success = fuseki.upload_turtle(GRAPH_VOCABULARY, turtle)
        if not stats.success:
            stats.error = "fuseki upload failed"
    except Exception as e:
        logger.exception("refresh_vocabulary failed for %s", scheme_uri)
        stats.error = str(e)
    return stats
