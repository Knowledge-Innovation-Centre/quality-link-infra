import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, SKOS, XSD

import requests

from services.vocabulary import language_tag_to_uri

from .base import (
    ADMS,
    DataSourceType,
    ELM,
    country_code_to_uri,
    currency_code_to_uri,
    isced_f_code_to_uri,
    org_uuid_from_value,
    get_date_datatype,
)

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")


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

    def _do_fetch(self, session):
        url = urljoin(self.source["path"], "courses")
        logger.info("OOAPI v%s request to %s", self.source["version"], url)

        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        params["pageSize"] = self.source.get("pageSize", 250)
        params["pageNumber"] = 0

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        success_count = 0
        failed_count = 0
        has_next_page = True

        while has_next_page:
            params["pageNumber"] += 1
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            items = data.get("items", [])
            page = data.get("pageNumber", 1)
            has_next_page = data.get("hasNextPage", False)

            logger.info("OOAPI page %s: %s courses", page, len(items))

            for course in items:
                # get list of offerings
                courseId = course.get("courseId")
                url_offerings = urljoin(self.source["path"], f"courses/{courseId}/offerings" )
                offerings = []
                params_offerings = {}
                if self.source.get("parameters"):
                    params_offerings.update(self.source["parameters"])
                params_offerings["pageSize"] = self.source.get("pageSize", 250)
                params_offerings["pageNumber"] = 0
                has_more_offerings = True
                while has_more_offerings:
                    params_offerings["pageNumber"] += 1
                    response_offerings = session.get(url_offerings, params=params_offerings, timeout=60)
                    if response_offerings.status_code == requests.codes.OK:
                        data_offerings = response_offerings.json()
                        offerings += data_offerings.get("items", [])
                        has_more_offerings = data_offerings.get("hasNextPage", False)
                    else:
                        has_more_offerings = False
                # convert to RDF
                if self.map_course_to_rdf(course, graph, offerings):
                    success_count += 1
                else:
                    failed_count += 1

        logger.info(
            "OOAPI fetch done: %s ok, %s failed, %s triples",
            success_count, failed_count, len(graph),
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

    def _course_organisation_uuid(self, course: Dict) -> Optional[str]:
        """Extract organisation UUID from `organisationId` or expanded `organisation.organisationId`."""
        org_id = course.get("organisationId")
        if not org_id:
            org = course.get("organisation")
            if isinstance(org, dict):
                org_id = org.get("organisationId")
        return org_uuid_from_value(org_id, owner=course.get("courseId") or course.get("offeringId"))

    def _programme_ids(self, course: Dict) -> List[str]:
        """Collect programme ids from `programmeIds[]` and expanded `programmes[*].programmeId`."""
        ids: List[str] = []
        for v in (course.get("programmeIds") or []):
            if isinstance(v, str) and v:
                ids.append(v)
        for p in (course.get("programmes") or []):
            if isinstance(p, dict) and isinstance(p.get("programmeId"), str):
                ids.append(p["programmeId"])
        # de-duplicate preserving order
        seen = set()
        unique: List[str] = []
        for i in ids:
            if i not in seen:
                seen.add(i)
                unique.append(i)
        return unique

    # --- main mapping ---------------------------------------------------

    def map_course_to_rdf(self, course: Dict, graph: Graph, offerings: List):
        courseId = course.get("courseId")
        if not courseId:
            return None

        course_uri = self._get_uri(courseId)
        course_uuid = self._get_uuid(courseId, course_uri)

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((course_uri, QL.sourceType, QL.OOAPISource))
        graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))
        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        # publisher (organisation)
        org_uuid = self._course_organisation_uuid(course)
        if org_uuid:
            graph.add((course_uri, DCTERMS.publisher, URIRef(f"urn:uuid:{org_uuid}")))

        # identifiers
        self._add_identifier_entry(graph, course_uri, course.get("primaryCode"))
        if course.get("abbreviation"):
            # plain notation, no schemeName / schemeId
            self._add_identifier(graph, course_uri, course["abbreviation"], scheme_name="OOAPI abbreviation")
        for other in (course.get("otherCodes") or []):
            self._add_identifier_entry(graph, course_uri, other)

        if course.get("name"):
            title = self.extract_english_value(course.get("name"))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang="en")))

        if course.get("description"):
            description = self.extract_english_value(course.get("description"))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang="en")))

        if course.get("learningOutcomes"):
            for outcome in course["learningOutcomes"]:
                lo = BNode()
                graph.add((lo, RDF.type, ELM.LearningOutcome))
                if self.source['version'] == '6':
                    graph.add((lo, DCTERMS.title, Literal(self.extract_english_value(outcome.get('name')), lang="en")))
                    if lo_desc := self.extract_english_value(outcome.get('description')):
                        lo_note = BNode()
                        graph.add((lo_note, RDF.type, ELM.Note))
                        graph.add((lo_note, ELM.noteLiteral, Literal(lo_desc, lang="en")))
                        graph.add((lo, ELM.additionalNote, lo_note))
                else:
                    graph.add((lo, DCTERMS.title, Literal(self.extract_english_value(outcome), lang="en")))
                graph.add((course_uri, ELM.learningOutcome, lo))

        study_load = course.get("studyLoad")
        if isinstance(study_load, dict) and study_load.get("value"):
            if study_load.get("studyLoadUnit", "ects") == "ects":
                ects = BNode()
                graph.add((ects, ELM.point, Literal(study_load["value"], datatype=XSD.double)))
                graph.add((ects, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((course_uri, ELM.creditPoint, ects))
            else:
                graph.add((course_uri, ELM.volumeOfLearning, Literal(study_load["value"], datatype=XSD.decimal)))

        # course duration as workload (xsd:duration). Distinct from studyLoad,
        # which feeds elm:creditPoint or volumeOfLearning depending on unit.
        if duration := course.get("duration"):
            graph.add((course_uri, ELM.volumeOfLearning, Literal(duration, datatype=XSD.duration)))

        self._value_to_concept(course, "level", graph, course_uri, ELM.EQFLevel, self.LEVEL_MAP)

        self._add_teaching_languages(graph, course_uri, course)

        if fields := course.get("fieldsOfStudy"):
            field_list = fields if isinstance(fields, list) else [fields]
            for field in field_list:
                if uri := isced_f_code_to_uri(field):
                    graph.add((course_uri, ELM.ISCEDFCode, uri))

        if course.get("link"):
            web = BNode()
            graph.add((web, RDF.type, ELM.WebResource))
            graph.add((web, ELM.contentUrl, Literal(course.get("link"))))
            graph.add((course_uri, FOAF.homepage, web))

        # Mode of delivery — null-safe; supports v6 plural and v5 singular.
        self._value_to_concept(course, "modeOfDelivery", graph, course_uri, ELM.mode, self.MODE_MAP)
        for mode in (course.get("modesOfDelivery") or []):
            if mode in self.MODE_MAP:
                graph.add((course_uri, ELM.mode, self.MODE_MAP[mode]))

        if course.get("admissionRequirements"):
            admission = BNode()
            graph.add((admission, RDF.type, ELM.Note))
            graph.add((admission, ELM.noteLiteral, Literal(
                self.extract_english_value(course["admissionRequirements"]), lang="en"
            )))
            graph.add((course_uri, ELM.entryRequirement, admission))

        # qualificationRequirements → additional note (separate semantics from admission)
        if qr := self.extract_english_value(course.get("qualificationRequirements")):
            note = BNode()
            graph.add((note, RDF.type, ELM.Note))
            graph.add((note, ELM.noteLiteral, Literal(qr, lang="en")))
            graph.add((course_uri, ELM.additionalNote, note))

        # enrolment[] → admission procedure (how to enrol, distinct from prerequisites)
        if enrol_text := self.extract_english_value(course.get("enrolment")):
            note = BNode()
            graph.add((note, RDF.type, ELM.Note))
            graph.add((note, ELM.noteLiteral, Literal(enrol_text, lang="en")))
            graph.add((course_uri, ELM.admissionProcedure, note))

        # assessment[] → provenBy a minimal LearningAssessmentSpecification
        if assessment_text := self.extract_english_value(course.get("assessment")):
            lass = BNode()
            graph.add((lass, RDF.type, ELM.LearningAssessmentSpecification))
            graph.add((lass, DCTERMS.description, Literal(assessment_text, lang="en")))
            graph.add((course_uri, ELM.provenBy, lass))

        # programmeIds[] / programmes[] → isPartOf (URI shared with the programme's own resource)
        for prog_id in self._programme_ids(course):
            graph.add((course_uri, ELM.isPartOf, self._get_uri(prog_id)))

        # supplementaryInformation: first type=image → bannerImage (sh:maxCount 1)
        self._add_first_banner_image(graph, course_uri, course)

        # Offerings of this course

        for offering in offerings:
            offeringId = offering.get("offeringId")
            offering_uri = URIRef(
                f"{course_uri}/offerings/{offeringId}"
            )
            graph.add((offering_uri, RDF.type, QL.LearningOpportunityInstance))
            graph.add((offering_uri, ELM.learningAchievementSpecification, course_uri))

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

            # addresses[] → elm:location (with optional elm:address / elm:countryCode)
            for address in (offering.get("addresses") or []):
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
                graph.add((offering_uri, ELM.location, loc))

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

        return course_uuid
