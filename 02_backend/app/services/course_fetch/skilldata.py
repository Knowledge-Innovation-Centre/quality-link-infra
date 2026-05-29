"""Skilldata-API integration for the silver stage.

Calls `POST {SKILLDATA_API_URL}/lo_generator/analyze_course` per LOS to
back-fill missing predicates required by the QL profile (`elm:learningOutcome`,
`elm:ISCEDFCode`, `dcterms:language`) and attach `elm:relatedESCOSkill` matches
to learning outcomes. Predicates already present in the source data are never
overwritten — the API result only fills gaps.

Each AI-populated predicate is recorded once on the course as
`ql:aiEnrichedField <predicate-uri>` so downstream consumers can tell which
parts came from the source and which from the AI tool.
"""
import logging
import textwrap
from typing import Any, Dict, List, Optional, Tuple

import requests
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD

from config import (
    SKILLDATA_API_KEY,
    SKILLDATA_API_TIMEOUT,
    SKILLDATA_API_URL,
    SKILLDATA_MAX_DISTANCE,
    SKILLDATA_SKILL_LIMIT,
)
from services.course_fetch.source_types.base import isced_f_code_to_uri
from services.vocabulary import language_tag_to_uri

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")

_ENDPOINT_PATH = "/lo_generator/analyze_course"
_MIN_INPUT_LEN = 40  # API contract: input_text minLength is 40


def is_configured() -> bool:
    """Feature is enabled whenever the API URL is set; key is optional."""
    return bool(SKILLDATA_API_URL)


# --- graph inspection ------------------------------------------------------


def _english_literal(graph: Graph, subject, predicate) -> Optional[str]:
    """Return the first English (lang='en') or untagged string literal."""
    fallback: Optional[str] = None
    for obj in graph.objects(subject, predicate):
        if isinstance(obj, Literal):
            if isinstance(obj.language, str) and obj.language.lower() == "en":
                return str(obj)
            if fallback is None and (obj.language is None or obj.language == ""):
                fallback = str(obj)
    return fallback


def _note_literal(graph: Graph, note_node) -> Optional[str]:
    return _english_literal(graph, note_node, ELM.noteLiteral)


def _existing_outcomes(graph: Graph, course_uri: URIRef) -> List[Tuple[Any, str]]:
    """Return existing (lo_node, text) pairs in graph order.

    Text is `title` plus, when present, `additionalNote` literal joined with
    " — ". Outcomes without any readable text are dropped.
    """
    out: List[Tuple[Any, str]] = []
    for lo in graph.objects(course_uri, ELM.learningOutcome):
        title = _english_literal(graph, lo, DCTERMS.title)
        notes: List[str] = []
        for note in graph.objects(lo, ELM.additionalNote):
            if text := _note_literal(graph, note):
                notes.append(text)
        parts = [p for p in [title, *notes] if p]
        if parts:
            out.append((lo, " — ".join(parts)))
    return out


def _outcomes_have_esco(graph: Graph, outcomes: List[Tuple[Any, str]]) -> bool:
    return any((lo, ELM.relatedESCOSkill, None) in graph for lo, _ in outcomes)


def _existing_language_tag(graph: Graph, course_uri: URIRef) -> Optional[str]:
    """Return the trailing 2- or 3-letter code from any dcterms:language URI."""
    for lang in graph.objects(course_uri, DCTERMS.language):
        if isinstance(lang, URIRef):
            tail = str(lang).rsplit("/", 1)[-1]
            if tail:
                return tail
    return None


def _existing_eqf_level_digit(graph: Graph, course_uri: URIRef) -> Optional[str]:
    """Return the trailing path segment of any elm:EQFLevel URI."""
    for level in graph.objects(course_uri, ELM.EQFLevel):
        if isinstance(level, URIRef):
            tail = str(level).rsplit("/", 1)[-1]
            if tail:
                return tail
    return None


def _assemble_input_text(
    graph: Graph,
    course_uri: URIRef,
    existing_outcomes: List[Tuple[Any, str]],
) -> str:
    parts: List[str] = []
    if title := _english_literal(graph, course_uri, DCTERMS.title):
        parts.append(title)
    if desc := _english_literal(graph, course_uri, DCTERMS.description):
        parts.append(desc)

    summary_texts: List[str] = []
    for note in graph.objects(course_uri, ELM.learningOutcomeSummary):
        if text := _note_literal(graph, note):
            summary_texts.append(text)
    if summary_texts:
        parts.append("Learning outcome summary:\n" + "\n".join(summary_texts))

    if existing_outcomes:
        lines = "\n".join(f"- {text}" for _, text in existing_outcomes)
        parts.append("Learning outcomes:\n" + lines)

    course_notes: List[str] = []
    for note in graph.objects(course_uri, ELM.additionalNote):
        if text := _note_literal(graph, note):
            course_notes.append(text)
    if course_notes:
        parts.append("Notes:\n" + "\n".join(course_notes))

    return "\n\n".join(parts).strip()


def _pick_mode(
    graph: Graph, course_uri: URIRef
) -> Tuple[str, List[Tuple[Any, str]]]:
    """Choose the API mode based on what the graph already contains."""
    existing = _existing_outcomes(graph, course_uri)
    if len(existing) >= 2:
        if _outcomes_have_esco(graph, existing):
            return "description_outcomes_skills", existing
        return "description_and_outcomes", existing
    return "description_only", existing


# --- API client ------------------------------------------------------------


def _call_api(payload: Dict[str, Any], session: requests.Session) -> Optional[Dict[str, Any]]:
    url = f"{SKILLDATA_API_URL.rstrip('/')}{_ENDPOINT_PATH}"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if SKILLDATA_API_KEY:
        headers["Kic-Api-Header"] = SKILLDATA_API_KEY
    try:
        r = session.post(url, json=payload, headers=headers, timeout=SKILLDATA_API_TIMEOUT)
    except requests.RequestException as e:
        logger.warning("skilldata: request failed: %s", e)
        return None
    if r.status_code != 200:
        body = r.text[:500] if r.text else ""
        logger.warning("skilldata: HTTP %s — %s", r.status_code, body)
        return None
    try:
        return r.json()
    except ValueError as e:
        logger.warning("skilldata: invalid JSON response: %s", e)
        return None


# --- response → graph ------------------------------------------------------


def _add_esco_skills(graph: Graph, lo_node, skills: List[Dict[str, Any]]) -> bool:
    """Add elm:relatedESCOSkill triples for skills with a non-empty skill_uri.

    Returns True if at least one triple was added.
    """
    added = False
    for skill in skills or []:
        skill_uri = skill.get("skill_uri") if isinstance(skill, dict) else None
        if skill_uri:
            graph.add((lo_node, ELM.relatedESCOSkill, URIRef(skill_uri)))
            added = True
    return added


def _apply_response(
    graph: Graph,
    course_uri: URIRef,
    mode: str,
    existing_outcomes: List[Tuple[Any, str]],
    response: Dict[str, Any],
) -> None:
    enriched_fields: set = set()
    response_outcomes = response.get("outcomes") or []

    # ESCO skill matching — modes 1 and 2
    skills_added = False
    if mode == "description_only":
        for entry in response_outcomes:
            if not isinstance(entry, dict):
                continue
            text = entry.get("outcome")
            if not isinstance(text, str) or not text.strip():
                continue
            lo = BNode()
            graph.add((lo, RDF.type, ELM.LearningOutcome))
            graph.add((lo, DCTERMS.title, Literal(text.strip(), lang="en")))
            graph.add((course_uri, ELM.learningOutcome, lo))
            enriched_fields.add(ELM.learningOutcome)
            if _add_esco_skills(graph, lo, entry.get("skills") or []):
                skills_added = True
    elif mode == "description_and_outcomes":
        for idx, entry in enumerate(response_outcomes):
            if not isinstance(entry, dict) or idx >= len(existing_outcomes):
                continue
            lo_node, _ = existing_outcomes[idx]
            if _add_esco_skills(graph, lo_node, entry.get("skills") or []):
                skills_added = True
    if skills_added:
        enriched_fields.add(ELM.relatedESCOSkill)

    # ISCED-F — only fill when missing
    if (course_uri, ELM.ISCEDFCode, None) not in graph:
        code = response.get("isced_f_code")
        if isinstance(code, str) and code:
            isced_uri = isced_f_code_to_uri(code)
            if isced_uri is not None:
                graph.add((course_uri, ELM.ISCEDFCode, isced_uri))
                enriched_fields.add(ELM.ISCEDFCode)

    # Language — only fill when missing
    if (course_uri, DCTERMS.language, None) not in graph:
        lang = response.get("language")
        if isinstance(lang, str) and lang:
            lang_uri = language_tag_to_uri(lang)
            if lang_uri is not None:
                graph.add((course_uri, DCTERMS.language, lang_uri))
                enriched_fields.add(DCTERMS.language)

    for predicate in enriched_fields:
        graph.add((course_uri, QL.aiEnrichedField, URIRef(predicate)))


# --- public entry point ----------------------------------------------------


def enrich_course_with_skilldata(
    graph: Graph,
    course_uri: URIRef,
    *,
    session: requests.Session,
) -> None:
    """Best-effort enrichment of one course in-place. Never raises."""
    try:
        mode, existing = _pick_mode(graph, course_uri)
        input_text = _assemble_input_text(graph, course_uri, existing)
        logger.debug(
            "skilldata: %s - input text:\n%s",
            course_uri, textwrap.indent(input_text, ' | '),
        )
        if len(input_text) < _MIN_INPUT_LEN:
            logger.info(
                "skilldata: %s — skipped (input text < %s chars)",
                course_uri, _MIN_INPUT_LEN,
            )
            return

        payload: Dict[str, Any] = {
            "input_text": input_text,
            "mode": mode,
            "limit": SKILLDATA_SKILL_LIMIT,
            "max_distance": SKILLDATA_MAX_DISTANCE,
        }
        if title := _english_literal(graph, course_uri, DCTERMS.title):
            payload["qualification_name"] = title
        if eqf := _existing_eqf_level_digit(graph, course_uri):
            payload["eqf_level"] = eqf
        if lang_tag := _existing_language_tag(graph, course_uri):
            payload["language"] = lang_tag
        if mode in ("description_and_outcomes", "description_outcomes_skills"):
            payload["outcomes"] = [text for _, text in existing]

        logger.info("skilldata: %s — mode=%s", course_uri, mode)
        response = _call_api(payload, session)
        if response is None:
            return

        _apply_response(graph, course_uri, mode, existing, response)
    except Exception as e:
        logger.exception("skilldata: %s — enrichment failed: %s", course_uri, e)
