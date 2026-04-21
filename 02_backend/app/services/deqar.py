import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, SKOS
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import DEQAR_API_URL, GRAPH_REFERENCE
from services import fuseki

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")
ROV = Namespace("http://www.w3.org/ns/regorg#")


@dataclass
class UpsertStats:
    total: int = 0
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    errors: int = 0
    data_updated: List[Tuple[str, Dict[str, Any]]] = field(default_factory=list)


@dataclass
class FusekiPushStats:
    success: int = 0
    failed: int = 0


def fetch_deqar_providers(
    limit: int = 2000,
    offset: int = 0,
    api_url: Optional[str] = None,
    *,
    max_retries: int = 3,
    retry_delay: int = 10,
    request_delay: int = 1,
) -> List[Dict[str, Any]]:
    """Page through the DEQAR provider API and return the combined list."""
    base = api_url or DEQAR_API_URL
    logger.info("Fetching DEQAR providers from %s (limit=%s, offset=%s)", base, limit, offset)

    results: List[Dict[str, Any]] = []
    more_pages = True
    total_count = 0

    with requests.Session() as session:
        while more_pages:
            retries = 0
            while True:
                try:
                    time.sleep(retry_delay if retries else request_delay)
                    response = session.get(base, params={"limit": limit, "offset": offset}, timeout=60)
                    if response.status_code == 200:
                        data = response.json()
                        total_count = data.get("count", total_count)
                        results.extend(data.get("results", []))
                        offset += limit
                        more_pages = bool(data.get("next"))
                        break
                    logger.warning("DEQAR HTTP %s for offset=%s", response.status_code, offset)
                except Exception as e:
                    logger.warning("DEQAR fetch error offset=%s: %s", offset, e)

                retries += 1
                if retries > max_retries:
                    logger.error("DEQAR: giving up on offset=%s after %s retries", offset, max_retries)
                    offset += limit
                    break

    logger.info("DEQAR fetch done: %s providers (total=%s)", len(results), total_count)
    return results


def _extract_schac(provider: Dict[str, Any]) -> Optional[str]:
    for identifier in provider.get("identifiers") or []:
        if identifier.get("resource") == "SCHAC":
            return identifier.get("identifier")
    return None


def _clean_website(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    return url.rstrip("/")


def _build_manifest_json(provider: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    schac_id = _extract_schac(provider)
    website_link = provider.get("website_link")
    clean = _clean_website(website_link)

    if schac_id:
        out.append({"domain": schac_id, "type": "DNS", "check": False, "path": None})
        out.append({"domain": schac_id, "type": ".well-known", "check": False, "path": None})
    if website_link:
        out.append({"domain": website_link, "type": "DNS", "check": False, "path": None})
        out.append({"domain": website_link, "type": ".well-known", "check": False, "path": None})
    if clean and clean != website_link:
        out.append({"domain": clean, "type": "DNS", "check": False, "path": None})
        out.append({"domain": clean, "type": ".well-known", "check": False, "path": None})
    return out


def _build_name_concat(provider: Dict[str, Any]) -> str:
    parts: List[str] = []
    if name_primary := provider.get("name_primary"):
        parts.append(name_primary)
    names = provider.get("names") or []
    if names:
        n = names[0]
        for key in ("name_official", "name_official_transliterated", "name_english", "acronym"):
            if v := n.get(key):
                parts.append(v)
    return " ".join(parts)


def upsert_providers(db: Session, providers: List[Dict[str, Any]]) -> UpsertStats:
    stats = UpsertStats()

    for provider in providers:
        stats.total += 1
        base_id = provider.get("id")
        try:
            existing = db.execute(
                text("SELECT provider_uuid, metadata FROM provider WHERE base_id = :bid"),
                {"bid": base_id},
            ).fetchone()

            if existing:
                if existing[1] != provider:
                    _update_provider(db, existing[0], provider)
                    stats.data_updated.append((str(existing[0]), provider))
                    stats.updated += 1
                else:
                    stats.unchanged += 1
            else:
                new_uuid = _insert_provider(db, provider)
                stats.data_updated.append((str(new_uuid), provider))
                stats.new += 1
            db.commit()
        except Exception as e:
            db.rollback()
            logger.warning("Provider upsert failed for base_id=%s: %s", base_id, e)
            stats.errors += 1

    logger.info(
        "Upsert: total=%s new=%s updated=%s unchanged=%s errors=%s",
        stats.total, stats.new, stats.updated, stats.unchanged, stats.errors,
    )
    return stats


def _insert_provider(db: Session, provider: Dict[str, Any]):
    params = {
        "deqar_id": provider.get("deqar_id"),
        "eter_id": provider.get("eter_id"),
        "base_id": provider.get("id"),
        "schac_code": _extract_schac(provider),
        "metadata": json.dumps(provider),
        "manifest_json": json.dumps(_build_manifest_json(provider)),
        "name_concat": _build_name_concat(provider),
        "provider_name": provider.get("name_primary", ""),
    }
    row = db.execute(
        text("""
            INSERT INTO provider (
                deqar_id, eter_id, base_id, schac_code, metadata, manifest_json,
                name_concat, provider_name, last_deqar_pull,
                last_manifest_pull, created_at, updated_at
            ) VALUES (
                :deqar_id, :eter_id, :base_id, :schac_code,
                CAST(:metadata AS jsonb), CAST(:manifest_json AS jsonb),
                :name_concat, :provider_name, NOW(),
                NULL, NOW(), NOW()
            )
            RETURNING provider_uuid
        """),
        params,
    ).fetchone()
    return row[0]


def _update_provider(db: Session, provider_uuid, provider: Dict[str, Any]) -> None:
    db.execute(
        text("""
            UPDATE provider
            SET deqar_id = :deqar_id,
                eter_id = :eter_id,
                schac_code = :schac_code,
                metadata = CAST(:metadata AS jsonb),
                name_concat = :name_concat,
                provider_name = :provider_name,
                last_deqar_pull = NOW(),
                updated_at = NOW()
            WHERE provider_uuid = :provider_uuid
        """),
        {
            "provider_uuid": provider_uuid,
            "deqar_id": provider.get("deqar_id"),
            "eter_id": provider.get("eter_id"),
            "schac_code": _extract_schac(provider),
            "metadata": json.dumps(provider),
            "name_concat": _build_name_concat(provider),
            "provider_name": provider.get("name_primary", ""),
        },
    )


def providers_to_rdf(stats: UpsertStats) -> List[Tuple[str, bytes]]:
    """Serialize each upserted provider to N-Triples ready for Fuseki push."""
    out: List[Tuple[str, bytes]] = []
    for provider_uuid, provider in stats.data_updated:
        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)
        graph.bind("rov", ROV)
        graph.bind("owl", OWL)
        provider_uri = _deqar_to_rdf(provider, provider_uuid, graph)
        out.append((provider_uri, graph.serialize(format="nt")))
    logger.info("Converted %s providers to RDF", len(out))
    return out


def _deqar_to_rdf(provider_source: Dict[str, Any], provider_uuid: str, graph: Graph) -> str:
    provider_uri = f"https://data.deqar.eu/institution/{provider_source['id']}"
    provider = {
        "@context": {
            "adms": "http://www.w3.org/ns/adms#",
            "dcterms": "http://purl.org/dc/terms/",
            "elm": "http://data.europa.eu/snb/model/elm/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "ql": "http://data.quality-link.eu/ontology/v1#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "name_primary": {"@id": "skos:prefLabel", "@language": "en"},
            "identifiers": {
                "@id": "adms:identifier",
                "@container": "@set",
                "@context": {
                    "identifier": "skos:notation",
                    "resource": "elm:schemeName",
                },
            },
        },
        "@type": "ql:HigherEducationInstitution",
        "@id": provider_uri,
        **provider_source,
    }

    if isinstance(provider.get("identifiers"), list):
        for i in provider["identifiers"]:
            if i.get("resource") == "SCHAC":
                i["@type"] = "ql:SchacIdentifier"
                i["elm:schemeId"] = {"@id": "ql:Schac"}
            else:
                i["@type"] = "elm:Identifier"

    graph.parse(data=json.dumps(provider), format="json-ld")
    hei = URIRef(provider_uri)

    graph.add((URIRef(f"urn:uuid:{provider_uuid}"), OWL.sameAs, hei))

    website = BNode()
    graph.add((hei, FOAF.homepage, website))
    graph.add((website, RDF.type, ELM.WebResource))
    graph.add((website, ELM.contentUrl, Literal(provider.get("website_link"))))

    deqar_id = BNode()
    graph.add((hei, ADMS.identifier, deqar_id))
    graph.add((deqar_id, RDF.type, ELM.Identifier))
    graph.add((deqar_id, SKOS.notation, Literal(provider.get("deqar_id"))))
    graph.add((deqar_id, ELM.schemeName, Literal("DEQARINST ID")))

    if provider.get("eter_id"):
        orgreg_id = BNode()
        graph.add((hei, ADMS.identifier, orgreg_id))
        graph.add((orgreg_id, RDF.type, QL.OrgRegIdentifier))
        graph.add((orgreg_id, SKOS.notation, Literal(provider["eter_id"])))
        graph.add((orgreg_id, ELM.schemeId, QL.OrgReg))
        graph.add((orgreg_id, ELM.schemeName, Literal("ETER ID")))

    for loc in provider.get("locations") or []:
        location = BNode()
        address = BNode()
        graph.add((hei, ELM.location, location))
        graph.add((location, RDF.type, DCTERMS.Location))
        graph.add((location, ELM.address, address))
        graph.add((address, RDF.type, ELM.Address))
        try:
            graph.add((address, ELM.countryCode, URIRef(
                f"http://publications.europa.eu/resource/authority/country/{loc['country']['iso_3166_alpha3']}"
            )))
        except (KeyError, TypeError):
            pass

    for n in provider.get("names") or []:
        if not n.get("name_valid_to"):
            if n.get("name_official"):
                graph.add((hei, ROV.legalName, Literal(n["name_official"])))
        else:
            if n.get("name_official"):
                graph.add((hei, SKOS.altLabel, Literal(n["name_official"])))
            if n.get("name_english"):
                graph.add((hei, SKOS.altLabel, Literal(n["name_english"])))

    return provider_uri


def push_providers_to_fuseki(rdf_list: List[Tuple[str, bytes]]) -> FusekiPushStats:
    stats = FusekiPushStats()
    with requests.Session() as session:
        for uri, nt_bytes in rdf_list:
            nt = nt_bytes.decode("utf-8") if isinstance(nt_bytes, bytes) else nt_bytes
            ok = fuseki.replace_subject_in_graph(GRAPH_REFERENCE, uri, nt, session=session)
            if ok:
                stats.success += 1
            else:
                stats.failed += 1
    logger.info("Fuseki push: success=%s failed=%s", stats.success, stats.failed)
    return stats
