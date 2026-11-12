"""Build EDC issuer upload templates from QualityLink course data.

The EDC issuer's "Upload JSON" feature takes a file of the shape

    {"credential": {...}, "deliveryDetails": {"deliveryAddress": [...]}}

where `credential` is an ELM credential in plain JSON (not JSON-LD). This module
maps one framed course (see `services.courses.frame_course`) onto such a
credential: the recipient holds a LearningAchievement awarded by the course's
publisher and specified by the course as Qualification or
LearningAchievementSpecification, optionally with one offering mapped in as its
LearningOpportunity.

The output is checked against the EDC generic JSON schema (vendored under
`schema/edc/`) with `@context` and `issuer` no longer required. The EDC issuer
supplies those itself.
"""
import base64
import copy
import json
import re
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from config import SCHEMA_DIR
from services.courses import _frame_config, frame_course
from services.edc_image import render_display_jpeg

EDC_SCHEMA_DIR = SCHEMA_DIR / "edc"

# Synthetic base for the vendored schemas, which carry no $id of their own: the
# EDC schema refers to "./VCschema1.1.json" relatively, so they need a shared base.
_SCHEMA_BASE = "https://data.quality-link.eu/schema/edc/"
_UPLOAD_SCHEMA = "edc-upload.schema.json"
_EDC_SCHEMA = "EDC-generic.jschema.json"
_VC_SCHEMA = "VCschema1.1.json"

CREDENTIAL_SCHEMA_ID = "http://data.europa.eu/snb/model/application-profile/edc-generic-full"
GENERIC_PROFILE = "http://data.europa.eu/snb/credential/e34929035b"
ENCODING_BASE64 = "http://data.europa.eu/snb/encoding/6146cde7dd"
FILE_TYPE_JPEG = "http://publications.europa.eu/resource/authority/file-type/JPEG"
LANGUAGE_AUTHORITY = "http://publications.europa.eu/resource/authority/language/"
ECTS_FRAMEWORK = "http://data.europa.eu/snb/education-credit/6fcec5c5af"

PLACEHOLDER_DELIVERY_ADDRESS = "email or wallet address"
PLACEHOLDER_GIVEN_NAME = "Given name"
PLACEHOLDER_FAMILY_NAME = "Family name"

# The language codes allowed as keys of an EDC LangString (from Many!LangStringType).
_LANG_KEY = re.compile(
    r"^(aa|ab|ae|af|ak|am|an|ar|as|av|ay|az|ba|be|bg|bh|bi|bm|bn|bo|br|bs|ca|ce|ch|co|"
    r"cr|cs|cu|cv|cy|da|de|dv|dz|ee|el|en|eo|es|et|eu|fa|ff|fi|fj|fo|fr|fy|ga|gd|gl|gn|"
    r"gu|gv|ha|he|hi|ho|hr|ht|hu|hy|hz|ia|id|ie|ig|ii|ik|in|io|is|it|iu|iw|ja|ji|jv|jw|"
    r"ka|kg|ki|kj|kk|kl|km|kn|ko|kr|ks|ku|kv|kw|ky|la|lb|lg|li|ln|lo|lt|lu|lv|mg|mh|mi|"
    r"mk|ml|mn|mo|mr|ms|mt|my|na|nb|nd|ne|ng|nl|nn|no|nr|nv|ny|oc|oj|om|or|os|pa|pi|pl|"
    r"ps|pt|qu|rm|rn|ro|ru|rw|sa|sc|sd|se|sg|sh|si|sk|sl|sm|sn|so|sq|sr|ss|st|su|sv|sw|"
    r"ta|te|tg|th|ti|tk|tl|tn|to|tr|ts|tt|tw|ty|ug|uk|ur|uz|ve|vi|vo|wa|wo|xh|yi|yo|za|"
    r"zh|zu)$"
)

# ISO 639-1 → EU language authority code, for the languages the course titles are
# likely to be in. Used to build displayParameter.language from title languages.
_LANG_AUTHORITY_CODES = {
    "bg": "BUL", "ca": "CAT", "cs": "CES", "cy": "CYM", "da": "DAN", "de": "DEU",
    "el": "ELL", "en": "ENG", "es": "SPA", "et": "EST", "eu": "EUS", "fi": "FIN",
    "fr": "FRA", "ga": "GLE", "gl": "GLG", "hr": "HRV", "hu": "HUN", "is": "ISL",
    "it": "ITA", "lb": "LTZ", "lt": "LIT", "lv": "LAV", "mk": "MKD", "mt": "MLT",
    "nb": "NOB", "nl": "NLD", "nn": "NNO", "no": "NOR", "pl": "POL", "pt": "POR",
    "ro": "RON", "ru": "RUS", "sk": "SLK", "sl": "SLV", "sq": "SQI", "sr": "SRP",
    "sv": "SWE", "tr": "TUR", "uk": "UKR",
}

DEFAULT_LANG = "en"


class InstanceNotFound(Exception):
    """The requested learning opportunity instance does not belong to the course."""


class EdcTemplateInvalid(Exception):
    """The generated template does not validate against the EDC upload schema."""

    def __init__(self, errors: List[str]):
        super().__init__(f"{len(errors)} schema violation(s)")
        self.errors = errors


# ---------------------------------------------------------------------------
# Schema loading and validation
# ---------------------------------------------------------------------------

def _load_schema(name: str) -> dict:
    with open(EDC_SCHEMA_DIR / name, "r") as f:
        return json.load(f)


def _without(required: List[str], *names: str) -> List[str]:
    return [r for r in required if r not in names]


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    """Validator for the upload file, with @context and issuer no longer required.

    The vendored EDC and VC schemas stay byte-identical to upstream, and the
    relaxation is applied here.
    """
    vc = _load_schema(_VC_SCHEMA)
    vc["required"] = _without(vc["required"], "@context", "issuer")

    edc = _load_schema(_EDC_SCHEMA)
    edc_credential = edc["$defs"]["EuropeanDigitalCredentialType"]
    edc_credential["required"] = _without(edc_credential["required"], "issuer")

    upload = _load_schema(_UPLOAD_SCHEMA)

    registry = Registry().with_resources(
        (_SCHEMA_BASE + name, Resource.from_contents(contents, default_specification=DRAFT202012))
        for name, contents in ((_VC_SCHEMA, vc), (_EDC_SCHEMA, edc), (_UPLOAD_SCHEMA, upload))
    )
    return Draft202012Validator(
        {"$ref": _SCHEMA_BASE + _UPLOAD_SCHEMA},
        registry=registry,
        format_checker=FormatChecker(),
    )


def validate_edc_template(doc: dict) -> List[str]:
    """Validate an upload file. Returns readable `path: message` errors (empty if valid)."""
    errors: List[str] = []
    for error in sorted(_validator().iter_errors(doc), key=lambda e: [str(p) for p in e.absolute_path]):
        leaf = _summarise(error)
        path = "/".join(str(p) for p in leaf.absolute_path) or "(root)"
        message = f"{path}: {leaf.message}"
        # The VC base schema and the EDC schema overlap (dates, id, type), so the
        # same violation is often reported twice.
        if message not in errors:
            errors.append(message)
    return errors


def _leaf_errors(error):
    if error.context:
        for sub in error.context:
            yield from _leaf_errors(sub)
    else:
        yield error


def _summarise(error):
    """The most telling leaf of an anyOf/allOf failure.

    The schema discriminates by `type` const (Agent | Person | Organisation) and
    by single-vs-array (`Many!…`), so most leaves only record that a branch did
    not apply. Drop those, then prefer the deepest remaining leaf.
    """
    leaves = list(_leaf_errors(error))
    telling = [
        e for e in leaves
        if not (e.validator == "const" and e.absolute_path and e.absolute_path[-1] == "type")
        and e.validator != "type"
    ] or leaves
    return max(telling, key=lambda e: len(e.absolute_path))


# ---------------------------------------------------------------------------
# Framed JSON-LD accessors
# ---------------------------------------------------------------------------

def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _first(value: Any) -> Any:
    values = _as_list(value)
    return values[0] if values else None


@lru_cache(maxsize=1)
def _prefixes() -> Dict[str, str]:
    """Prefixes of the JSON-LD frame's @context, used to compact the framed IRIs."""
    context = _frame_config().get("@context", {})
    return {k: v for k, v in context.items() if isinstance(v, str) and not k.startswith("@")}


def _iri(value: Any) -> Optional[str]:
    """The full IRI of a node or node reference; None for blank nodes and literals.

    Compaction against the frame's @context can leave IRIs as CURIEs (e.g.
    `ql:OrgReg`), which would pass the schema's `uri` format but mean nothing
    to the issuer, so they are expanded here.
    """
    if isinstance(value, dict):
        value = value.get("id") or value.get("@id")
    if not isinstance(value, str) or not value or value.startswith("_:"):
        return None
    prefix, sep, local = value.partition(":")
    if sep and not local.startswith("//") and prefix in _prefixes():
        return _prefixes()[prefix] + local
    return value


def _literal(value: Any) -> Optional[str]:
    """The lexical value of a literal (plain, typed or language-tagged)."""
    if isinstance(value, dict):
        value = value.get("@value")
    if value is None or isinstance(value, (dict, list)):
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _types(node: dict) -> List[str]:
    return [t for t in _as_list(node.get("type") or node.get("@type")) if isinstance(t, str)]


def _lang_key(tag: Optional[str]) -> Optional[str]:
    if not tag:
        return None
    key = tag.split("-")[0].lower()
    return key if _LANG_KEY.match(key) else None


def _lang_map(value: Any, default_lang: str = DEFAULT_LANG) -> Dict[str, List[str]]:
    """Framed literal(s) → EDC LangString map, e.g. {"en": ["…"]}.

    The frame compacts English strings to plain values (see the @language
    entries in schema/frame.json). Other languages stay {"@value", "@language"}.
    Untagged literals take `default_lang`; tags outside the EDC enum are dropped.
    """
    result: Dict[str, List[str]] = {}
    for item in _as_list(value):
        if isinstance(item, dict):
            text = item.get("@value")
            lang = _lang_key(item.get("@language")) if "@language" in item else default_lang
        else:
            text, lang = item, default_lang
        if not isinstance(text, str) or not text.strip() or not lang:
            continue
        result.setdefault(lang, []).append(text.strip())
    return result


def _single_lang(value: Dict[str, List[str]], preferred: str = DEFAULT_LANG) -> Dict[str, List[str]]:
    """Reduce a LangString map to one language (for LangStringType, maxProperties 1)."""
    if not value:
        return value
    lang = preferred if preferred in value else next(iter(value))
    return {lang: value[lang][:1]}


def _pick_text(value: Dict[str, List[str]], preferred: str = DEFAULT_LANG) -> Optional[str]:
    if not value:
        return None
    values = value.get(preferred) or next(iter(value.values()))
    return values[0] if values else None


# ---------------------------------------------------------------------------
# ELM node builders
# ---------------------------------------------------------------------------

class _Ids:
    """Mint `urn:epass:<kind>:<n>` ids, as the Online Credential Builder does.

    Blank nodes in the framed data come out as `_:b0`, which fails the schema's
    `uri` format, so nested nodes without an IRI get a minted one instead.
    """

    def __init__(self):
        self._counters: Dict[str, int] = {}

    def __call__(self, kind: str, node: Any = None) -> str:
        iri = _iri(node)
        if iri:
            return iri
        n = self._counters.get(kind, 0) + 1
        self._counters[kind] = n
        return f"urn:epass:{kind}:{n}"


def _concept_scheme(iri: str) -> Optional[str]:
    """Infer a controlled-list concept's scheme from its IRI.

    The EDC issuer needs only `id` and `inScheme.id` for controlled-list
    concepts, and looks the labels up itself.
    """
    m = re.match(r"^(http://data\.europa\.eu/snb/[^/]+)/[^/]+$", iri)
    if m and not iri.startswith("http://data.europa.eu/snb/model/"):
        return f"{m.group(1)}/25831c2"
    m = re.match(r"^(http://publications\.europa\.eu/resource/authority/[^/]+)/[^/]+$", iri)
    if m:
        return m.group(1)
    if iri.startswith("http://data.europa.eu/esco/skill/"):
        return "http://data.europa.eu/esco/concept-scheme/skills"
    return None


def _concept(node: Any, default_lang: str = DEFAULT_LANG) -> Optional[dict]:
    iri = _iri(node)
    if not iri:
        return None
    concept: Dict[str, Any] = {"id": iri, "type": "Concept"}
    scheme = _concept_scheme(iri)
    if scheme:
        concept["inScheme"] = {"id": scheme, "type": "ConceptScheme"}
    if isinstance(node, dict):
        label = _lang_map(node.get("skos:prefLabel"), default_lang)
        if label:
            concept["prefLabel"] = label
        notation = _literal(_first(node.get("skos:notation")))
        if notation:
            concept["notation"] = notation
    return concept


def _concepts(value: Any, default_lang: str = DEFAULT_LANG) -> List[dict]:
    return [c for c in (_concept(v, default_lang) for v in _as_list(value)) if c]


def _language_concept(lang: str) -> Optional[dict]:
    code = _LANG_AUTHORITY_CODES.get(lang)
    if not code:
        return None
    return _concept({"id": LANGUAGE_AUTHORITY + code})


def _note(node: Any, ids: _Ids, default_lang: str) -> Optional[dict]:
    if isinstance(node, dict):
        literal = _lang_map(node.get("elm:noteLiteral"), default_lang)
    else:
        literal = _lang_map(node, default_lang)
    if not literal:
        return None
    note: Dict[str, Any] = {"id": ids("note", node), "type": "Note", "noteLiteral": literal}
    if isinstance(node, dict):
        subject = _concept(node.get("dcterms:subject"), default_lang)
        if subject:
            note["subject"] = subject
    return note


def _notes(value: Any, ids: _Ids, default_lang: str) -> List[dict]:
    return [n for n in (_note(v, ids, default_lang) for v in _as_list(value)) if n]


def _web_resource(node: Any, ids: _Ids, default_lang: str) -> Optional[dict]:
    """foaf:homepage etc.: either an elm:WebResource node or a bare IRI."""
    if isinstance(node, dict) and ("elm:contentURL" in node or "elm:contentUrl" in node):
        url = _iri(node.get("elm:contentURL") or node.get("elm:contentUrl")) or _literal(
            node.get("elm:contentURL") or node.get("elm:contentUrl")
        )
        title = _lang_map(node.get("dcterms:title"), default_lang)
    else:
        url, title = _iri(node) or _literal(node), {}
    if not url or not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", url):
        return None
    resource: Dict[str, Any] = {"id": ids("webResource"), "type": "WebResource", "contentURL": url}
    if title:
        resource["title"] = title
    return resource


def _web_resources(value: Any, ids: _Ids, default_lang: str) -> List[dict]:
    return [w for w in (_web_resource(v, ids, default_lang) for v in _as_list(value)) if w]


def _identifier(node: Any, ids: _Ids) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    notation = _literal(_first(node.get("skos:notation")))
    if not notation:
        return None
    identifier: Dict[str, Any] = {"id": ids("identifier", node), "type": "Identifier", "notation": notation}
    scheme_name = _literal(_first(node.get("elm:schemeName")))
    if scheme_name:
        identifier["schemeName"] = scheme_name
    scheme_id = _iri(_first(node.get("elm:schemeId")))
    if scheme_id:
        identifier["schemeId"] = scheme_id
    return identifier


def _identifiers(value: Any, ids: _Ids) -> List[dict]:
    return [i for i in (_identifier(v, ids) for v in _as_list(value)) if i]


def _credit_point(node: Any, ids: _Ids) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    point = _literal(_first(node.get("elm:point")))
    framework = _concept(_first(node.get("elm:framework"))) or _concept({"id": ECTS_FRAMEWORK})
    if point is None:
        return None
    return {"id": ids("creditPoint", node), "type": "CreditPoint", "framework": framework, "point": point}


def _learning_outcome(node: Any, ids: _Ids, default_lang: str) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    title = _lang_map(node.get("dcterms:title"), default_lang)
    if not title:
        return None
    outcome: Dict[str, Any] = {"id": ids("learningOutcome", node), "type": "LearningOutcome", "title": title}
    skills = _concepts(node.get("elm:relatedESCOSkill"), default_lang)
    if skills:
        outcome["relatedESCOSkill"] = skills
    notes = _notes(node.get("elm:additionalNote"), ids, default_lang)
    if notes:
        outcome["additionalNote"] = notes
    return outcome


def _location(node: Any, ids: _Ids, default_lang: str) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    addresses = []
    for address in _as_list(node.get("elm:address")):
        if not isinstance(address, dict):
            continue
        country = _concept(_first(address.get("elm:countryCode")), default_lang)
        if not country:
            continue
        entry: Dict[str, Any] = {"id": ids("address", address), "type": "Address", "countryCode": country}
        full_address = _note(_first(address.get("elm:fullAddress")), ids, default_lang)
        if full_address:
            entry["fullAddress"] = full_address
        addresses.append(entry)
    if not addresses:
        return None
    location: Dict[str, Any] = {"id": ids("location", node), "type": "Location", "address": addresses}
    description = _lang_map(node.get("dcterms:description"), default_lang)
    if description:
        location["description"] = description
    return location


def _locations(value: Any, ids: _Ids, default_lang: str) -> List[dict]:
    return [loc for loc in (_location(v, ids, default_lang) for v in _as_list(value)) if loc]


def _organisation(node: Any, ids: _Ids, default_lang: str) -> dict:
    """The course publisher (a DEQAR institution, from the reference graph).

    Always returns an Organisation, even a bare one: a schema-valid but empty
    awardingBody would only fail later, in the issuer. A publisher without
    legalName or location therefore fails validation here, with a clear message.
    """
    if not isinstance(node, dict):
        node = {"id": _iri(node)} if _iri(node) else {}
    legal_name = _lang_map(node.get("regorg:legalName"), default_lang) or _lang_map(
        node.get("skos:prefLabel"), default_lang
    )
    org: Dict[str, Any] = {"id": ids("org", node), "type": "Organisation"}
    if legal_name:
        org["legalName"] = legal_name
    pref_label = _lang_map(node.get("skos:prefLabel"), default_lang)
    if pref_label and pref_label != legal_name:
        org["altLabel"] = pref_label
    locations = _locations(node.get("elm:location"), ids, default_lang)
    if locations:
        org["location"] = locations
    homepage = _web_resources(node.get("foaf:homepage"), ids, default_lang)
    if homepage:
        org["homepage"] = homepage
    identifiers = _identifiers(node.get("adms:identifier"), ids)
    if identifiers:
        org["identifier"] = identifiers
    return org


def _period_of_time(node: Any, ids: _Ids) -> Optional[dict]:
    if not isinstance(node, dict):
        return None
    period: Dict[str, Any] = {"id": ids("periodOfTime", node), "type": "PeriodOfTime"}
    for target in ("startDate", "endDate"):
        for source in (f"elm:{target}", f"dcat:{target}", f"http://www.w3.org/ns/dcat#{target}"):
            value = _as_datetime(_literal(_first(node.get(source))))
            if value:
                period[target] = value
                break
    return period if len(period) > 2 else None


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$")


def _as_datetime(value: Optional[str]) -> Optional[str]:
    """Normalise to an RFC 3339 date-time, which the EDC schema uses for all dates.

    Dates get midnight UTC, and local times are read as UTC. Anything else,
    such as the "01/05/2025" some sources publish, is dropped rather than guessed.
    """
    if not value:
        return None
    value = value.strip()
    if _ISO_DATE.match(value):
        return f"{value}T00:00:00Z"
    m = _ISO_DATETIME.match(value)
    if not m:
        return None
    return value if m.group(2) else value + "Z"


def _duration(value: Optional[str]) -> Optional[str]:
    """An ISO 8601 duration, or None for all-zero placeholders such as "P0D"."""
    if not value or not value.startswith("P") or not re.search(r"[1-9]", value):
        return None
    return value


def _set(target: dict, key: str, value: Any) -> None:
    """Assign only non-empty values, so optional properties stay absent."""
    if value not in (None, "", [], {}):
        target[key] = value


# ---------------------------------------------------------------------------
# Course → credential
# ---------------------------------------------------------------------------

def _specification(course: dict, ids: _Ids, lang: str) -> dict:
    is_qualification = "elm:Qualification" in _types(course)
    spec: Dict[str, Any] = {
        "id": _iri(course) or ids("learningAchievementSpecification"),
        "type": "Qualification" if is_qualification else "LearningAchievementSpecification",
        "title": _lang_map(course.get("dcterms:title"), lang),
    }
    _set(spec, "altLabel", _lang_map(course.get("skos:altLabel"), lang))
    _set(spec, "description", _lang_map(course.get("dcterms:description"), lang))
    _set(spec, "identifier", _first(_identifiers(course.get("adms:identifier"), ids)))
    _set(spec, "homepage", _web_resources(course.get("foaf:homepage"), ids, lang))
    _set(spec, "supplementaryDocument", _web_resources(course.get("elm:supplementaryDocument"), ids, lang))
    _set(spec, "language", _concepts(course.get("dcterms:language"), lang))
    _set(spec, "dcType", _concepts(course.get("dcterms:type"), lang))
    _set(spec, "volumeOfLearning", _duration(_literal(_first(course.get("elm:volumeOfLearning")))))
    _set(spec, "learningOutcomeSummary", _note(_first(course.get("elm:learningOutcomeSummary")), ids, lang))
    _set(spec, "learningOutcome", [
        o for o in (_learning_outcome(n, ids, lang) for n in _as_list(course.get("elm:learningOutcome"))) if o
    ])
    _set(spec, "creditPoint", [
        c for c in (_credit_point(n, ids) for n in _as_list(course.get("elm:creditPoint"))) if c
    ])
    _set(spec, "educationSubject", _concepts(course.get("elm:ISCEDFCode"), lang))
    _set(spec, "entryRequirement", _note(_first(course.get("elm:entryRequirement")), ids, lang))
    _set(spec, "additionalNote", _notes(course.get("elm:additionalNote"), ids, lang))
    _set(spec, "targetGroup", _concepts(course.get("elm:targetGroup"), lang))
    _set(spec, "category", _lang_map(course.get("elm:category"), lang))
    _set(spec, "mode", _concepts(course.get("elm:mode"), lang))
    _set(spec, "dateModified", _as_datetime(_literal(_first(course.get("dcterms:modified")))))

    eqf = _concept(_first(course.get("elm:EQFLevel")), lang)
    if is_qualification:
        _set(spec, "eqfLevel", eqf)
        _set(spec, "nqfLevel", _concepts(course.get("elm:NQFLevel"), lang))
        _set(spec, "qualificationCode", _concepts(course.get("elm:qualificationCode"), lang))
        partial = _first(course.get("elm:isPartialQualification"))
        if isinstance(partial, bool):
            spec["isPartialQualification"] = partial
    elif eqf:
        spec["educationLevel"] = [eqf]

    return spec


def _learning_opportunity(
    instance: dict, course: dict, publisher_org: Optional[dict], ids: _Ids, lang: str
) -> dict:
    title = _lang_map(instance.get("dcterms:title"), lang) or _lang_map(course.get("dcterms:title"), lang)
    opportunity: Dict[str, Any] = {
        "id": ids("learningOpportunity", instance),
        "type": "LearningOpportunity",
        "title": title,
    }
    _set(opportunity, "description", _lang_map(instance.get("dcterms:description"), lang))
    _set(opportunity, "identifier", _identifiers(instance.get("adms:identifier"), ids))
    _set(opportunity, "homepage", _web_resources(instance.get("foaf:homepage"), ids, lang))
    _set(opportunity, "temporal", _period_of_time(_first(instance.get("dcterms:temporal")), ids))
    _set(opportunity, "duration", _duration(_literal(_first(instance.get("elm:duration")))))
    _set(opportunity, "mode", _concepts(instance.get("elm:mode"), lang))
    _set(opportunity, "learningSchedule", _concept(_first(instance.get("elm:learningSchedule")), lang))
    _set(opportunity, "scheduleInformation", _note(_first(instance.get("elm:scheduleInformation")), ids, lang))
    _set(opportunity, "admissionProcedure", _note(_first(instance.get("elm:admissionProcedure")), ids, lang))
    _set(opportunity, "additionalNote", _notes(instance.get("elm:additionalNote"), ids, lang))
    _set(opportunity, "location", _locations(instance.get("elm:location"), ids, lang))
    _set(opportunity, "defaultLanguage", _concept(
        _first(instance.get("elm:defaultLanguage")) or _first(instance.get("dcterms:language")), lang
    ))
    _set(opportunity, "applicationDeadline", [
        d for d in (_as_datetime(_literal(v)) for v in _as_list(instance.get("elm:applicationDeadline"))) if d
    ])
    # The frame trims providers to a label, which is short of the legalName and
    # location an EDC Organisation requires. Only the publisher is described in full.
    provider = _iri(_first(instance.get("elm:providedBy")))
    if publisher_org and provider and provider == publisher_org.get("id"):
        opportunity["providedBy"] = copy.deepcopy(publisher_org)
    return opportunity


def _find_instance(course: dict, instance_uri: str) -> dict:
    for instance in _as_list(course.get("elm:learningOpportunity")):
        if isinstance(instance, dict) and _iri(instance) == instance_uri:
            return instance
    raise InstanceNotFound(f"Course has no learning opportunity instance {instance_uri}")


def _display_details(spec: dict, org: Optional[dict], lang: str) -> List[str]:
    details: List[str] = []
    for credit in spec.get("creditPoint", []):
        framework = _pick_text(credit["framework"].get("prefLabel", {}), lang)
        if credit["framework"]["id"] == ECTS_FRAMEWORK:
            framework = "ECTS"
        details.append(f"{credit['point']} {framework or 'credits'}")
    eqf = spec.get("eqfLevel") or _first(spec.get("educationLevel"))
    if eqf:
        label = _pick_text(eqf.get("prefLabel", {}), lang)
        details.append(f"EQF {label}" if label and not label.upper().startswith("EQF") else (label or eqf["id"]))
    subjects = [s for s in (_pick_text(c.get("prefLabel", {}), lang) for c in spec.get("educationSubject", [])) if s]
    if subjects:
        details.append(", ".join(subjects))
    languages = [s for s in (_pick_text(c.get("prefLabel", {}), lang) for c in spec.get("language", [])) if s]
    if languages:
        details.append("Language: " + ", ".join(languages))
    if org:
        for location in org.get("location", []):
            for address in location.get("address", []):
                country = _pick_text(address["countryCode"].get("prefLabel", {}), lang)
                if country:
                    details.append(country)
                    break
            break
    return details


def build_edc_template(
    course_uri: str,
    *,
    delivery_addresses: Optional[Iterable[str]] = None,
    given_name: Optional[str] = None,
    family_name: Optional[str] = None,
    instance_uri: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an EDC issuer upload file for one course.

    Raises `CourseNotFound` (from `frame_course`) and `InstanceNotFound`.
    Does not validate; call `validate_edc_template` on the result.
    """
    course = frame_course(course_uri)
    ids = _Ids()

    title = _lang_map(course.get("dcterms:title"))
    lang = DEFAULT_LANG if DEFAULT_LANG in title else next(iter(title), DEFAULT_LANG)

    spec = _specification(course, ids, lang)
    org = _organisation(_first(course.get("dcterms:publisher")), ids, lang)

    achievement: Dict[str, Any] = {
        "id": ids("learningAchievement"),
        "type": "LearningAchievement",
        "title": title,
        "awardedBy": {
            "id": ids("awardingProcess"),
            "type": "AwardingProcess",
            "awardingBody": [org],
        },
        "specifiedBy": spec,
    }
    if instance_uri:
        achievement["learningOpportunity"] = _learning_opportunity(
            _find_instance(course, instance_uri), course, org, ids, lang
        )

    given = given_name or PLACEHOLDER_GIVEN_NAME
    family = family_name or PLACEHOLDER_FAMILY_NAME
    subject = {
        "id": ids("person"),
        "type": "Person",
        "givenName": {lang: [given]},
        "familyName": {lang: [family]},
        "fullName": {lang: [f"{given} {family}"]},
        "hasClaim": [achievement],
    }

    title_text = _pick_text(title, lang) or course_uri
    org_name = _pick_text(org.get("legalName", {}), lang) if org else None
    image = base64.b64encode(
        render_display_jpeg(title_text, org_name, _display_details(spec, org, lang))
    ).decode("ascii")

    languages = [c for c in (_language_concept(code) for code in title) if c]
    primary = next((c for c in languages if c["id"].endswith("/ENG")), _first(languages)) or _language_concept(DEFAULT_LANG)
    if not languages:
        languages = [primary]

    individual_display = []
    for language in languages:
        individual_display.append({
            "id": ids("individualDisplay"),
            "type": "IndividualDisplay",
            "language": language,
            "displayDetail": [{
                "id": ids("displayDetail"),
                "type": "DisplayDetail",
                "image": {
                    "id": ids("mediaObject"),
                    "type": "MediaObject",
                    "content": image,
                    "contentEncoding": _concept({"id": ENCODING_BASE64}),
                    "contentType": _concept({"id": FILE_TYPE_JPEG}),
                },
                "page": 1,
            }],
        })

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    credential = {
        "id": f"urn:credential:{uuid.uuid4()}",
        "type": ["VerifiableCredential", "EuropeanDigitalCredential"],
        "credentialSchema": [{"id": CREDENTIAL_SCHEMA_ID, "type": "ShaclValidator2017"}],
        "credentialProfiles": [_concept({"id": GENERIC_PROFILE})],
        "credentialSubject": subject,
        "issuanceDate": now,
        "issued": now,
        "validFrom": now,
        "displayParameter": {
            "id": ids("displayParameter"),
            "type": "DisplayParameter",
            "title": title,
            "language": languages,
            "primaryLanguage": primary,
            "individualDisplay": individual_display,
        },
    }

    addresses = [a.strip() for a in (delivery_addresses or []) if a and a.strip()]
    return {
        "credential": credential,
        "deliveryDetails": {"deliveryAddress": addresses or [PLACEHOLDER_DELIVERY_ADDRESS]},
    }
