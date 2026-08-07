import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, RDF, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, SKOS, XSD

import requests

from services.vocabulary import language_tag_to_uri

from .base import (
    ADMS,
    DataSourceType,
    ELM,
    QL,
    add_inverse_part_links,
    country_code_to_uri,
    currency_code_to_uri,
    isced_f_code_to_uri,
    org_uuid_from_value,
    get_date_datatype,
)

logger = logging.getLogger(__name__)


class OoapiDataSource(DataSourceType):
    """OOAPI (v5 and v6) data source."""

    # EQF level URIs (EU snb)
    _EQF = {n: URIRef(f"http://data.europa.eu/snb/eqf/{n}") for n in range(1, 9)}

    # Levels: covers both OOAPI v5 (legacy spaced keys) and v6 (snake_case).
    # Some v6 values (pre_vocational, post_doctoral, undefined, undivided,
    # nt2_*) intentionally have no EQF mapping and fall through silently.
    LEVEL_MAP = {
        # v5 / spaced (existing)
        "secondary vocational education 1": _EQF[1],
        "secondary vocational education 2": _EQF[2],
        "secondary vocational education 3": _EQF[3],
        "secondary vocational education 4": _EQF[4],
        "associate degree": _EQF[5],
        # shared between v5 and v6
        "bachelor": _EQF[6],
        "master": _EQF[7],
        "doctoral": _EQF[8],
        # v6 underscore
        "secondary_vocational_education": _EQF[4],  # umbrella → highest sub-level
        "secondary_vocational_education_1": _EQF[1],
        "secondary_vocational_education_2": _EQF[2],
        "secondary_vocational_education_3": _EQF[3],
        "secondary_vocational_education_4": _EQF[4],
        "associate_degree": _EQF[5],
    }

    # Modes of delivery → EU snb learning-assessment concepts.
    # Unknown values fall through without raising.
    _MODE_ONLINE     = URIRef("http://data.europa.eu/snb/learning-assessment/920fbb3cbe")
    _MODE_PRESENTIAL = URIRef("http://data.europa.eu/snb/learning-assessment/9191af2ed9")
    _MODE_HYBRID     = URIRef("http://data.europa.eu/snb/learning-assessment/c_3a90b26d")
    MODE_MAP = {
        # v5
        "distance-learning": _MODE_ONLINE,
        "on campus":         _MODE_PRESENTIAL,
        "online":            _MODE_ONLINE,
        "hybrid":            _MODE_HYBRID,
        "situated":          _MODE_PRESENTIAL,
        # v6
        "blended":    _MODE_HYBRID,     # no distinct concept; closest fit
        "coil":       _MODE_ONLINE,
        "presential": _MODE_PRESENTIAL,
        # v6 values with no clean ELM fit (joint_delivery, project_based,
        # research_lab_based, work_based) intentionally not mapped.
    }

    # codeType → elm:schemeId URI (only mappings we're confident in).
    # All other codeTypes are emitted as elm:schemeName literal only.
    _SCHEME_ID_MAP = {
        "schac_home": QL.Schac,
    }

    # programmeType → EU learning-opportunity concept. The axis is
    # standalone-programme vs. component-of-a-programme: OOAPI defines minor,
    # specialisation, track and specification as sitting *within* a broader
    # programme, and "Programme module" is the only EU concept for a part of a
    # programme. The vocabulary has no concept for a programme variant or
    # pathway, so for track/specification this is the closest available fit
    # rather than an exact one. Unmapped and `x-` custom values fall back to
    # PROGRAMME_TYPE.
    #
    # This assigns a type only. No elm:specialisationOf is inferred from it:
    # OOAPI has no canonical way to express specialisation, and a
    # specialisation-typed programme nested under a parent is not reliably a
    # specialisation *of that parent* in the ELM sense.
    PROGRAMME_TYPE_MAP = {
        "programme":      DataSourceType.PROGRAMME_TYPE,
        "honours":        DataSourceType.PROGRAMME_TYPE,
        "minor":          DataSourceType.PROGRAMME_MODULE_TYPE,
        "specialisation": DataSourceType.PROGRAMME_MODULE_TYPE,
        "track":          DataSourceType.PROGRAMME_MODULE_TYPE,
        "specification":  DataSourceType.PROGRAMME_MODULE_TYPE,
    }

    # levelOfQualification → EQF. A more reliable EQF signal than the `level`
    # enum, so it wins when both are present.
    # Deliberately unmapped: `eqf_0` (the EQF has no level 0) and `nlqf_4plus`,
    # which the OOAPI spec itself describes as "above EQF level 4 but not
    # formally mapped to EQF level 5".
    # Spelled out rather than built by comprehension: a comprehension in a
    # class body gets its own scope and cannot see _EQF.
    LEVEL_OF_QUALIFICATION_MAP = {
        "eqf_1": _EQF[1],
        "eqf_2": _EQF[2],
        "eqf_3": _EQF[3],
        "eqf_4": _EQF[4],
        "eqf_5": _EQF[5],
        "eqf_6": _EQF[6],
        "eqf_7": _EQF[7],
        "eqf_8": _EQF[8],
    }

    # modeOfStudy → elm:learningSchedule. Only full_time maps: the EU
    # learning-schedule vocabulary has just three concepts (full time, part
    # time intensive <8-30h/week>, part time light <<8h/week>) and a bare
    # `part_time` carries no hours to choose between the two. dual_training and
    # self_paced have no schedule equivalent.
    MODE_OF_STUDY_MAP = {
        "full_time": URIRef("http://data.europa.eu/snb/learning-schedule/72a0ab92fa"),
    }

    # formalDocument is an enum of document kinds, not a document reference, so
    # it becomes a readable note rather than an elm:supplementaryDocument.
    _FORMAL_DOCUMENT_LABELS = {
        "certificate":                  "Certificate",
        "diploma":                      "Diploma",
        "micro_credential_certificate": "Micro-credential certificate",
        "school_advice":                "School advice",
        "testimonial":                  "Testimonial",
        "no_official_document":         "No official document",
    }

    _QUALIFICATION_AWARDED_LABELS = {
        "diploma":            "Diploma",
        "vocational_diploma": "Vocational diploma",
        "certificate":        "Certificate",
        "associate_degree":   "Associate degree",
        "bachelor":           "Bachelor",
        "master":             "Master",
        "doctoral":           "Doctoral degree",
        "none":               "No formal qualification",
    }

    # OOAPI v6 resultValueType → human label used as the GradingScheme title.
    _RESULT_VALUE_TYPE_LABELS = {
        "pass_or_fail":                   "Pass or fail",
        "insufficient_satisfactory_good": "Insufficient / satisfactory / good",
        "us_letter":                      "US letter (A–F)",
        "uk_letter":                      "UK letter (A–E, U)",
        "de_grade":                       "German scale (1–6)",
        "grade_0_100":                    "Numeric 0–100",
        "grade_0_10":                     "Numeric 0–10",
        "grade_0_10_one_decimal":         "Numeric 0–10 (one decimal)",
        "reference_level_europass":       "Europass reference level (A1–C2)",
    }

    def _iter_pages(self, session, url: str, *, tolerate_errors: bool = False):
        """Yield `(page_number, items)` for an OOAPI paginated collection.

        Pages are 1-based and the response's `hasNextPage` flag terminates the
        walk. With `tolerate_errors`, a non-200 ends the walk quietly instead of
        raising — used for sub-collections and for endpoints a provider may
        simply not implement, where a miss must not fail the whole run.
        """
        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        params["pageSize"] = self.source.get("pageSize", 250)

        page_number = 0
        while True:
            page_number += 1
            params["pageNumber"] = page_number
            response = session.get(url, params=params, timeout=60)
            if tolerate_errors and response.status_code != requests.codes.OK:
                if page_number == 1:
                    logger.info(
                        "OOAPI %s returned %s — skipping this collection",
                        url, response.status_code,
                    )
                return
            response.raise_for_status()
            data = response.json()
            yield data.get("pageNumber", page_number), data.get("items", [])
            if not data.get("hasNextPage", False):
                return

    def _fetch_collection(
        self, session, graph: Graph, los_subjects: set, *,
        collection: str, id_field: str, offerings_path: str, mapper, label: str,
        tolerate_missing: bool = False,
    ):
        """Page a top-level collection, fetch each item's offerings, map both.

        Returns `(mapped, failed)`.
        """
        url = urljoin(self.source["path"], collection)
        logger.info("OOAPI v%s request to %s", self.source["version"], url)

        mapped = 0
        failed = 0
        for page, items in self._iter_pages(
            session, url, tolerate_errors=tolerate_missing
        ):
            logger.info("OOAPI page %s: %s %ss", page, len(items), label)
            for item in items:
                item_id = item.get(id_field)
                offerings = []
                if item_id:
                    offerings_url = urljoin(
                        self.source["path"], f"{collection}/{item_id}/{offerings_path}"
                    )
                    offerings = [
                        offering
                        for _, batch in self._iter_pages(
                            session, offerings_url, tolerate_errors=True
                        )
                        for offering in batch
                    ]
                if mapper(item, graph, offerings):
                    mapped += 1
                    los_subjects.add(self._get_uri(item_id))
                else:
                    failed += 1
        return mapped, failed

    def _do_fetch(self, session):
        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        los_subjects: set = set()

        courses_ok, courses_failed = self._fetch_collection(
            session, graph, los_subjects,
            collection="courses", id_field="courseId",
            offerings_path="offerings",
            mapper=self.map_course_to_rdf, label="course",
        )
        # /programmes is tolerated as missing: it is absent from OOAPI v5 and
        # not every v6 provider implements it, and a 404 here must not fail an
        # otherwise good course fetch.
        programmes_ok, programmes_failed = self._fetch_collection(
            session, graph, los_subjects,
            collection="programmes", id_field="programmeId",
            offerings_path="programme-offerings",
            mapper=self.map_programme_to_rdf, label="programme",
            tolerate_missing=True,
        )

        dangling = add_inverse_part_links(graph, los_subjects)
        if dangling:
            logger.info(
                "OOAPI: %s part link(s) reference a learning opportunity this "
                "source does not publish; left as a one-way reference",
                dangling,
            )

        logger.info(
            "OOAPI fetch done: %s courses ok, %s programmes ok, %s failed, %s triples",
            courses_ok, programmes_ok, courses_failed + programmes_failed, len(graph),
        )
        return graph.serialize(format="turtle", encoding="utf-8"), "text/turtle"

    # --- helpers -------------------------------------------------------

    def _add_identifier_entry(self, graph: Graph, subject: URIRef, entry: Any) -> None:
        """Map an OOAPI `IdentifierEntry` (`code` + `codeType`) to adms:identifier."""
        if not isinstance(entry, dict):
            return
        code = entry.get("code")
        code_type = entry.get("codeType")
        self._add_identifier(
            graph, subject, code,
            scheme_name=code_type,
            scheme_id=self._SCHEME_ID_MAP.get(code_type),
        )

    def _add_teaching_languages(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """Emit dcterms:language for v6 `teachingLanguages[]` and/or v5 `teachingLanguage`."""
        tags: List[str] = []
        v6 = source.get("teachingLanguages")
        if isinstance(v6, list):
            tags.extend(t for t in v6 if isinstance(t, str))
        v5 = source.get("teachingLanguage")
        if isinstance(v5, str):
            tags.append(v5)
        for tag in tags:
            uri = language_tag_to_uri(tag)
            if isinstance(uri, URIRef):
                graph.add((subject, DCTERMS.language, uri))

    def _add_first_banner_image(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """Take the first supplementaryInformation entry with type=image and emit elm:bannerImage."""
        for item in (source.get("supplementaryInformation") or []):
            if not isinstance(item, dict) or item.get("type") != "image":
                continue
            for v in (item.get("value") or []):
                if isinstance(v, dict) and v.get("value"):
                    media = BNode()
                    graph.add((media, RDF.type, ELM.MediaObject))
                    graph.add((media, ELM.contentUrl, Literal(v["value"])))
                    graph.add((subject, ELM.bannerImage, media))
                    return

    def _add_supplementary_notes(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """Walk supplementaryInformation[] non-image entries: text_* → elm:additionalNote, uri/video → elm:supplementaryDocument."""
        _TEXT_TYPES = {"text_md", "text_plain", "text_http"}
        _URL_TYPES = {"uri", "video"}
        for item in (source.get("supplementaryInformation") or []):
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type in _TEXT_TYPES:
                text = self.extract_english_value(item.get("value"))
                if not text:
                    continue
                note = BNode()
                graph.add((note, RDF.type, ELM.Note))
                graph.add((note, ELM.noteLiteral, Literal(text, lang="en")))
                graph.add((subject, ELM.additionalNote, note))
            elif item_type in _URL_TYPES:
                for v in (item.get("value") or []):
                    url = v.get("value") if isinstance(v, dict) else None
                    if not url:
                        continue
                    doc = BNode()
                    graph.add((doc, RDF.type, ELM.WebResource))
                    graph.add((doc, ELM.contentUrl, Literal(url)))
                    graph.add((subject, ELM.supplementaryDocument, doc))

    def _course_organisation_uuid(self, course: Dict) -> Optional[str]:
        """Extract organisation UUID from `organisationId` or expanded `organisation.organisationId`."""
        org_id = course.get("organisationId")
        if not org_id:
            org = course.get("organisation")
            if isinstance(org, dict):
                org_id = org.get("organisationId")
        return org_uuid_from_value(org_id, owner=course.get("courseId") or course.get("offeringId"))

    def _collect_ids(
        self, source: Dict, id_key: str, expanded_key: str, id_field: str = "programmeId",
    ) -> List[str]:
        """Collect referenced ids from an id field and/or its expanded form.

        OOAPI exposes most references twice: as a plain id (`programmeIds`,
        `childIds`, `parentId`) and, when the client asks for expansion, as the
        full object (`programmes`, `children`, `parent`). Either may be a scalar
        or a list, so both shapes are accepted and the result de-duplicated with
        order preserved.
        """
        ids: List[str] = []
        raw = source.get(id_key)
        for v in (raw if isinstance(raw, list) else [raw]):
            if isinstance(v, str) and v:
                ids.append(v)
        expanded = source.get(expanded_key)
        for item in (expanded if isinstance(expanded, list) else [expanded]):
            if isinstance(item, dict):
                value = item.get(id_field)
                if isinstance(value, str) and value:
                    ids.append(value)
        seen = set()
        unique: List[str] = []
        for i in ids:
            if i not in seen:
                seen.add(i)
                unique.append(i)
        return unique

    def _programme_ids(self, course: Dict) -> List[str]:
        """Collect programme ids from `programmeIds[]` and expanded `programmes[*].programmeId`."""
        return self._collect_ids(course, "programmeIds", "programmes")

    def _add_addresses(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """addresses[] → elm:location (with optional elm:address / elm:countryCode)."""
        for address in (source.get("addresses") or []):
            if not isinstance(address, dict):
                continue
            loc = BNode()
            graph.add((loc, RDF.type, DCTERMS.Location))
            if city := address.get("city"):
                graph.add((loc, ELM.geographicName, Literal(city, lang="en")))
            cc = address.get("countryCode")
            if isinstance(cc, dict):
                country_code = cc.get("iso3166-1-alpha3") or cc.get("iso3166-1-alpha2")
                country_uri = country_code_to_uri(country_code)
                if country_uri is not None:
                    addr_node = BNode()
                    graph.add((addr_node, RDF.type, ELM.Address))
                    graph.add((addr_node, ELM.countryCode, country_uri))
                    graph.add((loc, ELM.address, addr_node))
            graph.add((subject, ELM.location, loc))

    def _add_note(self, graph: Graph, subject: URIRef, predicate: URIRef, text: str) -> None:
        """Attach an elm:Note blank node carrying `text` to `subject`."""
        if not text:
            return
        note = BNode()
        graph.add((note, RDF.type, ELM.Note))
        graph.add((note, ELM.noteLiteral, Literal(text, lang="en")))
        graph.add((subject, predicate, note))

    # --- main mapping ---------------------------------------------------

    def _new_los(self, graph: Graph, source_id: str):
        """Create the shared LOS skeleton for a course or programme.

        Returns `(uri, uuid)`. dcterms:type is left to the caller — it is the
        only thing that distinguishes a course from a programme in this model,
        since the QualityLink profile gives both the same class.
        """
        uri = self._get_uri(source_id)
        los_uuid = self._get_uuid(source_id, uri)
        graph.add((uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((uri, QL.sourceType, QL.OOAPISource))
        graph.add((URIRef(f"urn:uuid:{los_uuid}"), OWL.sameAs, uri))
        return uri, los_uuid

    def _map_los_common(self, source: Dict, graph: Graph, subject: URIRef) -> None:
        """Map the fields OOAPI `CourseProperties` and `ProgrammeProperties` share.

        Every field touched here exists on both schemas, which is what lets the
        programme path reuse the course mapping wholesale.
        """
        # publisher (organisation)
        org_uuid = self._course_organisation_uuid(source)
        if org_uuid:
            graph.add((subject, DCTERMS.publisher, URIRef(f"urn:uuid:{org_uuid}")))

        # identifiers
        self._add_identifier_entry(graph, subject, source.get("primaryCode"))
        if source.get("abbreviation"):
            # plain notation, no schemeName / schemeId
            self._add_identifier(graph, subject, source["abbreviation"], scheme_name="OOAPI abbreviation")
        for other in (source.get("otherCodes") or []):
            self._add_identifier_entry(graph, subject, other)

        if source.get("name"):
            title = self.extract_english_value(source.get("name"))
            if title:
                graph.add((subject, DCTERMS.title, Literal(title, lang="en")))

        if source.get("description"):
            description = self.extract_english_value(source.get("description"))
            if description:
                graph.add((subject, DCTERMS.description, Literal(description, lang="en")))

        self._add_learning_outcomes(graph, subject, source)
        self._add_study_load(graph, subject, source)

        # duration as workload (xsd:duration). Distinct from studyLoad, which
        # feeds elm:creditPoint or volumeOfLearning depending on unit.
        if duration := source.get("duration"):
            graph.add((subject, ELM.volumeOfLearning, Literal(duration, datatype=XSD.duration)))

        self._value_to_concept(source, "level", graph, subject, ELM.EQFLevel, self.LEVEL_MAP)

        self._add_teaching_languages(graph, subject, source)

        if fields := source.get("fieldsOfStudy"):
            field_list = fields if isinstance(fields, list) else [fields]
            for field in field_list:
                if uri := isced_f_code_to_uri(field):
                    graph.add((subject, ELM.ISCEDFCode, uri))

        if source.get("link"):
            web = BNode()
            graph.add((web, RDF.type, ELM.WebResource))
            graph.add((web, ELM.contentUrl, Literal(source.get("link"))))
            graph.add((subject, FOAF.homepage, web))

        # Mode of delivery — null-safe; supports v6 plural and v5 singular.
        self._value_to_concept(source, "modeOfDelivery", graph, subject, ELM.mode, self.MODE_MAP)
        for mode in (source.get("modesOfDelivery") or []):
            if mode in self.MODE_MAP:
                graph.add((subject, ELM.mode, self.MODE_MAP[mode]))

        if source.get("admissionRequirements"):
            self._add_note(
                graph, subject, ELM.entryRequirement,
                self.extract_english_value(source["admissionRequirements"]),
            )

        # qualificationRequirements → additional note (separate semantics from admission)
        self._add_note(
            graph, subject, ELM.additionalNote,
            self.extract_english_value(source.get("qualificationRequirements")),
        )

        # enrolment[] → admission procedure (how to enrol, distinct from prerequisites)
        self._add_note(
            graph, subject, ELM.admissionProcedure,
            self.extract_english_value(source.get("enrolment")),
        )

        # assessment[] → provenBy a minimal LearningAssessmentSpecification
        if assessment_text := self.extract_english_value(source.get("assessment")):
            lass = BNode()
            graph.add((lass, RDF.type, ELM.LearningAssessmentSpecification))
            graph.add((lass, DCTERMS.description, Literal(assessment_text, lang="en")))
            graph.add((subject, ELM.provenBy, lass))

        # supplementaryInformation: first type=image → bannerImage (sh:maxCount 1)
        self._add_first_banner_image(graph, subject, source)
        # supplementaryInformation non-image entries → additionalNote / supplementaryDocument
        self._add_supplementary_notes(graph, subject, source)

        # resources[] → single additionalNote (free-text list of readings)
        resources = source.get("resources")
        if isinstance(resources, list):
            items = [str(r).strip() for r in resources if isinstance(r, str) and r.strip()]
            if items:
                self._add_note(
                    graph, subject, ELM.additionalNote,
                    "Resources:\n- " + "\n- ".join(items),
                )

    def _add_study_load(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """studyLoad → elm:creditPoint (ECTS) or elm:volumeOfLearning.

        OOAPI v5 sends a single StudyLoadDescriptor object; v6 sends an array of
        them (for both courses and programmes). Both shapes are accepted — the
        v6 array form was previously ignored, so v6 sources emitted no credit
        points at all.
        """
        study_load = source.get("studyLoad")
        if isinstance(study_load, dict):
            entries = [study_load]
        elif isinstance(study_load, list):
            entries = [e for e in study_load if isinstance(e, dict)]
        else:
            return

        for entry in entries:
            if not entry.get("value"):
                continue
            if entry.get("studyLoadUnit", "ects") == "ects":
                ects = BNode()
                graph.add((ects, ELM.point, Literal(entry["value"], datatype=XSD.double)))
                graph.add((ects, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((subject, ELM.creditPoint, ects))
            else:
                graph.add((subject, ELM.volumeOfLearning, Literal(entry["value"], datatype=XSD.decimal)))

    def _add_learning_outcomes(self, graph: Graph, subject: URIRef, source: Dict) -> None:
        """learningOutcomes[] → elm:learningOutcome. v6 sends objects, v5 strings."""
        if not source.get("learningOutcomes"):
            return
        for outcome in source["learningOutcomes"]:
            lo = BNode()
            graph.add((lo, RDF.type, ELM.LearningOutcome))
            if self.source['version'] == '6':
                graph.add((lo, DCTERMS.title, Literal(self.extract_english_value(outcome.get('name')), lang="en")))
                if lo_desc := self.extract_english_value(outcome.get('description')):
                    lo_note = BNode()
                    graph.add((lo_note, RDF.type, ELM.Note))
                    graph.add((lo_note, ELM.noteLiteral, Literal(lo_desc, lang="en")))
                    graph.add((lo, ELM.additionalNote, lo_note))
                if abbr := outcome.get('abbreviation'):
                    graph.add((lo, SKOS.altLabel, Literal(abbr, lang="en")))
                if lo_field := outcome.get('fieldsOfStudy'):
                    if uri := isced_f_code_to_uri(lo_field):
                        graph.add((lo, ELM.ISCEDFCode, uri))
                for other in (outcome.get('otherCodes') or []):
                    self._add_identifier_entry(graph, lo, other)
            else:
                graph.add((lo, DCTERMS.title, Literal(self.extract_english_value(outcome), lang="en")))
            graph.add((subject, ELM.learningOutcome, lo))

    def map_course_to_rdf(self, course: Dict, graph: Graph, offerings: List):
        courseId = course.get("courseId")
        if not courseId:
            return None

        course_uri, course_uuid = self._new_los(graph, courseId)
        graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))

        self._map_los_common(course, graph, course_uri)

        # programmeIds[] / programmes[] → isPartOf (URI shared with the
        # programme's own resource). The reverse elm:hasPart is materialised
        # once per fetch by add_inverse_part_links.
        for prog_id in self._programme_ids(course):
            graph.add((course_uri, ELM.isPartOf, self._get_uri(prog_id)))

        for offering in offerings:
            self._map_offering(offering, graph, course_uri)

        return course_uuid

    def map_programme_to_rdf(self, programme: Dict, graph: Graph, offerings: List):
        """Map an OOAPI Programme to a LOS, reusing the course field mapping.

        A programme is not a distinct class in ELM or the QualityLink profile —
        it is the same ql:LearningOpportunitySpecification with a different
        dcterms:type and a populated hasPart/isPartOf relation.
        """
        programmeId = programme.get("programmeId")
        if not programmeId:
            return None

        programme_uri, programme_uuid = self._new_los(graph, programmeId)
        self._add_type(
            graph, programme_uri, programme.get("programmeType"),
            self.PROGRAMME_TYPE_MAP, self.PROGRAMME_TYPE,
        )

        self._map_los_common(programme, graph, programme_uri)

        # levelOfQualification is an explicit EQF statement, so it overrides
        # whatever the coarser `level` enum contributed in _map_los_common.
        level_of_qualification = self.LEVEL_OF_QUALIFICATION_MAP.get(
            programme.get("levelOfQualification")
        )
        if level_of_qualification is not None:
            graph.remove((programme_uri, ELM.EQFLevel, None))
            graph.add((programme_uri, ELM.EQFLevel, level_of_qualification))

        self._value_to_concept(
            programme, "modeOfStudy", graph, programme_uri,
            ELM.learningSchedule, self.MODE_OF_STUDY_MAP,
        )

        # qualificationAwarded / qualificationDesignations / formalDocument all
        # describe the credential earned. ELM has no property for "type of
        # qualification awarded" on a specification, so they become notes.
        awarded = self._QUALIFICATION_AWARDED_LABELS.get(programme.get("qualificationAwarded"))
        designations = [
            d.strip() for d in (programme.get("qualificationDesignations") or [])
            if isinstance(d, str) and d.strip()
        ]
        if awarded or designations:
            text = " ".join(filter(None, [awarded or "Qualification", *designations]))
            self._add_note(graph, programme_uri, ELM.additionalNote,
                           f"Qualification awarded: {text}")

        if document := self._FORMAL_DOCUMENT_LABELS.get(programme.get("formalDocument")):
            self._add_note(graph, programme_uri, ELM.additionalNote,
                           f"Formal document: {document}")

        self._add_addresses(graph, programme_uri, programme)

        # Programme hierarchy. Both directions come free in the payload, so no
        # extra request is needed; add_inverse_part_links fills in whichever
        # side the source omitted.
        for parent_id in self._collect_ids(programme, "parentId", "parent"):
            graph.add((programme_uri, ELM.isPartOf, self._get_uri(parent_id)))
        for child_id in self._collect_ids(programme, "childIds", "children"):
            graph.add((programme_uri, ELM.hasPart, self._get_uri(child_id)))

        for offering in offerings:
            self._map_offering(offering, graph, programme_uri)

        return programme_uuid

    def _map_offering(self, offering: Dict, graph: Graph, parent_uri: URIRef) -> None:
        """Map one offering to a LOS instance hanging off `parent_uri`.

        Serves both course offerings and programme offerings: in OOAPI,
        `CourseOffering` and `ProgrammeOffering` compose the same
        `OfferingProperties` schema, so one mapping covers both.
        """
        offeringId = offering.get("offeringId")
        offering_uri = URIRef(
            f"{parent_uri}/offerings/{offeringId}"
        )
        graph.add((offering_uri, RDF.type, QL.LearningOpportunityInstance))
        graph.add((offering_uri, ELM.learningAchievementSpecification, parent_uri))

        # providedBy from offering organisation (only when UUID)
        offering_org_uuid = self._course_organisation_uuid(offering)
        if offering_org_uuid:
            graph.add((offering_uri, ELM.providedBy, URIRef(f"urn:uuid:{offering_org_uuid}")))

        # identifiers on the offering
        self._add_identifier_entry(graph, offering_uri, offering.get("primaryCode"))
        if offering.get("abbreviation"):
            self._add_identifier(graph, offering_uri, offering["abbreviation"], scheme_name="OOAPI abbreviation")
        for other in (offering.get("otherCodes") or []):
            self._add_identifier_entry(graph, offering_uri, other)

        if offering.get("name"):
            title = self.extract_english_value(offering.get("name"))
            if title:
                graph.add((offering_uri, DCTERMS.title, Literal(title, lang="en")))
        if offering.get("description"):
            description = self.extract_english_value(offering.get("description"))
            if description:
                graph.add((offering_uri, DCTERMS.description, Literal(description, lang="en")))

        self._add_teaching_languages(graph, offering_uri, offering)

        # temporal: prefer offering's own start/end (v6 …DateTime, v5 …Date),
        # else fall back to the linked academicSession's dates / name.
        session = offering.get("academicSession") if isinstance(offering.get("academicSession"), dict) else None
        start = offering.get("startDateTime") or offering.get("startDate")
        end = offering.get("endDateTime") or offering.get("endDate")
        session_start = session.get("startDateTime") if session else None
        session_end = session.get("endDateTime") if session else None
        session_name = self.extract_english_value(session.get("name")) if session else ""

        if start or end or session_start or session_end or session_name:
            temporal = BNode()
            graph.add((temporal, RDF.type, DCTERMS.PeriodOfTime))
            effective_start = start or session_start
            effective_end = end or session_end
            if effective_start:
                graph.add((temporal, ELM.startDate, Literal(effective_start, datatype=get_date_datatype(effective_start))))
            if effective_end:
                graph.add((temporal, ELM.endDate, Literal(effective_end, datatype=get_date_datatype(effective_end))))
            if session_name:
                graph.add((temporal, SKOS.prefLabel, Literal(session_name, lang="en")))
            graph.add((offering_uri, DCTERMS.temporal, temporal))

        self._value_to_concept(offering, "modeOfDelivery", graph, offering_uri, ELM.mode, self.MODE_MAP)
        for mode in (offering.get("modesOfDelivery") or []):
            if mode in self.MODE_MAP:
                graph.add((offering_uri, ELM.mode, self.MODE_MAP[mode]))

        self._value_to_literal(offering, "maxNumberStudents",       graph, offering_uri, QL.enrolmentCapacity,      datatype=XSD.nonNegativeInteger)
        self._value_to_literal(offering, "enrolledNumberStudents",  graph, offering_uri, QL.enrolledLearnerCount,   datatype=XSD.nonNegativeInteger)
        self._value_to_literal(offering, "minNumberStudents",       graph, offering_uri, QL.enrolmentMinimum,       datatype=XSD.nonNegativeInteger)

        self._add_addresses(graph, offering_uri, offering)

        # priceInformation[] → elm:priceDetail
        for cost in (offering.get("priceInformation") or []):
            if not isinstance(cost, dict) or not cost.get("amount"):
                continue
            price = BNode()
            graph.add((price, RDF.type, ELM.PriceDetail))
            amt = BNode()
            graph.add((amt, RDF.type, ELM.Amount))
            graph.add((amt, ELM.value, Literal(cost["amount"], datatype=XSD.decimal)))
            if currency_uri := currency_code_to_uri(cost.get("currency")):
                graph.add((amt, ELM.unit, currency_uri))
            graph.add((price, ELM.amount, amt))
            graph.add((offering_uri, ELM.priceDetail, price))

        # enrolmentPeriods[] → applicationDeadline / registrationPortal / additionalNote
        for period in (offering.get("enrolmentPeriods") or []):
            if not isinstance(period, dict):
                continue
            if pend := period.get("endDateTime"):
                graph.add((offering_uri, ELM.applicationDeadline, Literal(pend, datatype=XSD.dateTime)))
            if purl := period.get("enrolmentUrl"):
                graph.add((offering_uri, QL.registrationPortal, Literal(purl, datatype=XSD.anyURI)))
            if pcomment := period.get("comment"):
                note = BNode()
                graph.add((note, RDF.type, ELM.Note))
                graph.add((note, ELM.noteLiteral, Literal(pcomment, lang="en")))
                graph.add((offering_uri, ELM.additionalNote, note))

        # state → ql:isActive (only when present)
        state = offering.get("state")
        if state:
            graph.add((offering_uri, QL.isActive, Literal(state == "active", datatype=XSD.boolean)))

        if offering.get("link"):
            web = BNode()
            graph.add((web, RDF.type, ELM.WebResource))
            graph.add((web, ELM.contentUrl, Literal(offering.get("link"))))
            graph.add((offering_uri, FOAF.homepage, web))

        self._add_first_banner_image(graph, offering_uri, offering)
        self._add_supplementary_notes(graph, offering_uri, offering)

        # flexibleEntryPeriodStartDateTime/EndDateTime → elm:scheduleInformation
        # (single Note, sh:maxCount 1). Distinct from dcterms:temporal, which
        # holds the offering's fixed start/end.
        flex_start = offering.get("flexibleEntryPeriodStartDateTime")
        flex_end = offering.get("flexibleEntryPeriodEndDateTime")
        if flex_start or flex_end:
            if flex_start and flex_end:
                text = f"Flexible entry: {flex_start} to {flex_end}"
            elif flex_start:
                text = f"Flexible entry from {flex_start}"
            else:
                text = f"Flexible entry until {flex_end}"
            sched = BNode()
            graph.add((sched, RDF.type, ELM.Note))
            graph.add((sched, ELM.noteLiteral, Literal(text, lang="en")))
            graph.add((offering_uri, ELM.scheduleInformation, sched))

        # resultValueType → elm:gradingScheme (title-only GradingScheme)
        rvt = offering.get("resultValueType")
        rvt_label = self._RESULT_VALUE_TYPE_LABELS.get(rvt)
        if rvt_label:
            scheme = BNode()
            graph.add((scheme, RDF.type, ELM.GradingScheme))
            graph.add((scheme, DCTERMS.title, Literal(rvt_label, lang="en")))
            graph.add((offering_uri, ELM.gradingScheme, scheme))
