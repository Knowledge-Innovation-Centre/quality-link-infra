import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, SKOS, XSD

from services.vocabulary import language_tag_to_uri

from .base import (
    ADMS,
    DataSourceType,
    ELM,
    isced_f_code_to_uri,
    org_uuid_from_value,
    get_date_datatype,
)

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")


# ---------------------------------------------------------------------------
# Learning-schedule URI placeholders
#
# The EU snb `learning-schedule` vocabulary defines 3 concepts (part-time
# light, part-time intensive, full-time). Fill in the actual URIs below to
# enable paceOfStudy → elm:learningSchedule mapping. With these left as None
# the mapping silently skips (and logs a one-time warning at module load).
# ---------------------------------------------------------------------------
_SCHEDULE_PART_TIME =           URIRef("http://data.europa.eu/snb/learning-schedule/67395e6b5a") # < 8h/week
_SCHEDULE_PART_TIME_INTENSIVE = URIRef("http://data.europa.eu/snb/learning-schedule/f230bae523") # 8–30h/week
_SCHEDULE_FULL_TIME =           URIRef("http://data.europa.eu/snb/learning-schedule/72a0ab92fa") # > 30h/week


def _pace_to_schedule(pace: str) -> Optional[URIRef]:
    """Map an Edu-API paceOfStudy percentage to an elm:learningSchedule URI.

    Buckets follow LOQ: <20% ≈ <8h/40h → part-time low; 20–75% ≈ 8–30h/40h →
    part-time intensive; >75% ≈ >30h/40h → full-time.
    """
    if not isinstance(pace, str):
        return None
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*%?\s*$", pace)
    if not m:
        return None
    pct = float(m.group(1))
    if pct < 20:
        return _SCHEDULE_PART_TIME
    if pct <= 75:
        return _SCHEDULE_PART_TIME_INTENSIVE
    return _SCHEDULE_FULL_TIME


class EduApiDataSource(DataSourceType):
    """Edu-API (v1) data source."""

    _EQF = {n: URIRef(f"http://data.europa.eu/snb/eqf/{n}") for n in range(1, 9)}

    # `ext:eqf:N` keys allow providers to express EQF levels directly via the
    # Edu-API `level` extension mechanism, since the standard enum only
    # covers undergraduate/graduate/doctoral.
    LEVEL_MAP = {
        "undergraduate": _EQF[6],
        "graduate":      _EQF[7],
        "doctoral":      _EQF[8],
        "ext:eqf:1": _EQF[1],
        "ext:eqf:2": _EQF[2],
        "ext:eqf:3": _EQF[3],
        "ext:eqf:4": _EQF[4],
        "ext:eqf:5": _EQF[5],
        "ext:eqf:6": _EQF[6],
        "ext:eqf:7": _EQF[7],
        "ext:eqf:8": _EQF[8],
    }

    MODE_MAP = {
        "online":   URIRef("http://data.europa.eu/snb/learning-assessment/920fbb3cbe"), # Online
        "blended":  URIRef("http://data.europa.eu/snb/learning-assessment/c_3a90b26d"), # Hybrid
        "onGround": URIRef("http://data.europa.eu/snb/learning-assessment/9191af2ed9"), # Presential
    }

    # Only mappings with a confirmed EU snb learning-opportunity concept are
    # listed here. Other courseType values (standard, honors, research,
    # independentStudy, practicum, studyAbroad, capstone, clinical,
    # correspondence, fieldExperience, seminar) fall through to COURSE_TYPE
    # since the vocabulary fit hasn't been verified yet.
    COURSE_TYPE_MAP = {
        "internship": URIRef("http://data.europa.eu/snb/learning-opportunity/77b99de990"),
        "thesis":     URIRef("http://data.europa.eu/snb/learning-opportunity/b2434ca358"),
    }

    def _do_fetch(self, session):
        url = urljoin(self.source["path"], "courseTemplates")
        url_offerings = urljoin(self.source["path"], "courseOfferings")

        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        params["limit"] = self.source.get("pageSize", 500)

        logger.info("Edu-API request to %s", url)

        params["offset"] = 0
        has_next_page = True

        courses = {}
        offerings = {}

        while has_next_page:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            items = response.json()

            if len(items) >= params["limit"]:
                params["offset"] += params["limit"]
            else:
                has_next_page = False

            logger.info("Edu-API page: %s courses", len(items))

            for course in items:
                courses[course.get('sourcedId')] = course
                offerings[course.get('sourcedId')] = []

        logger.info("Edu-API request to %s", url_offerings)

        params["offset"] = 0
        has_next_page = True

        while has_next_page:
            response = session.get(url_offerings, params=params, timeout=60)
            response.raise_for_status()
            items = response.json()

            if len(items) >= params["limit"]:
                params["offset"] += params["limit"]
            else:
                has_next_page = False

            logger.info("Edu-API page: %s course offerings", len(items))

            for offering in items:
                if offering.get('course') in offerings:
                    offerings[offering.get('course')].append(offering)
                else:
                    logger.warning(f"- courseOffering {offering['sourcedId']} refers to unknown courseTemplate {offering['course']}")

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        success_count = 0
        failed_count = 0

        for course_id in courses:
            if self.map_course_to_rdf(courses[course_id], graph, offerings[course_id]):
                success_count += 1
            else:
                failed_count += 1

        logger.info(
            "Edu-API fetch done: %s ok, %s failed, %s triples",
            success_count, failed_count, len(graph),
        )
        return graph.serialize(format="turtle", encoding="utf-8"), "text/turtle"

    def extract_english_value(self, multilingual_field: Any) -> str:
        if isinstance(multilingual_field, str):
            return multilingual_field
        if not isinstance(multilingual_field, list) or not multilingual_field:
            return ""
        for item in multilingual_field:
            if isinstance(item, dict):
                lang = item.get("language", "").lower()
                if "en" in lang:
                    return item.get("value", "")
        if isinstance(multilingual_field[0], dict):
            return multilingual_field[0].get("value", "")
        return str(multilingual_field[0])

    # --- helpers -------------------------------------------------------

    def _add_identifier_entry(self, graph: Graph, subject: URIRef, entry: Any) -> None:
        """Map an Edu-API `IdentifierEntry` (`identifier` + `identifierType`) to adms:identifier."""
        if not isinstance(entry, dict):
            return
        self._add_identifier(
            graph, subject,
            entry.get("identifier"),
            scheme_name=entry.get("identifierType"),
        )

    def _add_record_status(self, graph: Graph, subject: URIRef, value: Optional[str]) -> None:
        """recordStatus → ql:isActive. `active` → True; `inactive`/`deleted` → False."""
        if value not in ("active", "inactive", "deleted"):
            return
        graph.add((subject, QL.isActive, Literal(value == "active", datatype=XSD.boolean)))

    # --- main mapping --------------------------------------------------

    def map_course_to_rdf(self, course: Dict, graph: Graph, offerings: List):
        courseId = course.get("sourcedId")
        if not courseId:
            return None

        course_uri = self._get_uri(courseId)
        course_uuid = self._get_uuid(courseId, course_uri)

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((course_uri, QL.sourceType, QL.EduApiSource))
        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        # type
        if course.get("courseType") in self.COURSE_TYPE_MAP:
            graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE_MAP[course["courseType"]]))
        else:
            graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))

        # offering organisation
        org_uuid = org_uuid_from_value(course.get("organization"), owner=courseId)
        if org_uuid:
            graph.add((course_uri, DCTERMS.publisher, URIRef(f"urn:uuid:{org_uuid}")))

        # identifiers
        self._add_identifier_entry(graph, course_uri, course.get("primaryCode"))
        for other in (course.get("otherCodes") or []):
            self._add_identifier_entry(graph, course_uri, other)

        if course.get("title"):
            title = self.extract_english_value(course.get("title"))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang="en")))

        if course.get("description"):
            description = self.extract_english_value(course.get("description"))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang="en")))

        self._value_to_concept(course, "level", graph, course_uri, ELM.EQFLevel, self.LEVEL_MAP)

        # subjectCodes[]: ISCED-F where it looks like a numeric ISCED code
        # (4-digit codes pass through; 5–6 digit SOI2021 codes auto-truncate
        # to the parent 4-digit ISCED concept), otherwise educationSubject.
        for code in (course.get("subjectCodes") or []):
            if not isinstance(code, str) or not code:
                continue
            if uri := isced_f_code_to_uri(code):
                graph.add((course_uri, ELM.ISCEDFCode, uri))
            else:
                subj = BNode()
                graph.add((subj, RDF.type, SKOS.Concept))
                graph.add((subj, SKOS.notation, Literal(code)))
                graph.add((course_uri, ELM.educationSubject, subj))

        # try to parse credits
        if course.get("creditType") == "credit" and "creditsAwarded" in course:
            if m := re.match(r"\d+(\.\d+)?", course["creditsAwarded"]):
                credit = BNode()
                graph.add((credit, ELM.point, Literal(m[0], datatype=XSD.double)))
                if re.search("ECTS", course["creditsAwarded"], re.IGNORECASE):
                    graph.add((credit, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((course_uri, ELM.creditPoint, credit))

        if course.get("teachingLanguage"):
            lang_uri = language_tag_to_uri(course.get("teachingLanguage"))
            if isinstance(lang_uri, URIRef):
                graph.add((course_uri, DCTERMS.language, lang_uri))

        # gradingScheme[] → elm:gradingScheme (single blank node, English title)
        if scheme_title := self.extract_english_value(course.get("gradingScheme")):
            scheme = BNode()
            graph.add((scheme, RDF.type, ELM.GradingScheme))
            graph.add((scheme, DCTERMS.title, Literal(scheme_title, lang="en")))
            graph.add((course_uri, ELM.gradingScheme, scheme))

        # recordStatus → ql:isActive
        self._add_record_status(graph, course_uri, course.get("recordStatus"))

        # dateLastModified → dcterms:modified
        if dlm := course.get("dateLastModified"):
            graph.add((course_uri, DCTERMS.modified, Literal(dlm, datatype=XSD.dateTime)))

        # parent[] (CollectionTemplate refs) → elm:isPartOf, URI shared with the collection's own resource
        for parent_id in (course.get("parent") or []):
            if isinstance(parent_id, str) and parent_id:
                graph.add((course_uri, ELM.isPartOf, self._get_uri(parent_id)))

        # Offerings of this course

        for offering in offerings:
            offeringId = offering.get("sourcedId")
            offering_uri = URIRef(
                f"{course_uri}/offerings/{offeringId}"
            )
            graph.add((offering_uri, RDF.type, QL.LearningOpportunityInstance))
            graph.add((offering_uri, ELM.learningAchievementSpecification, course_uri))

            offering_org_uuid = org_uuid_from_value(offering.get("organization"), owner=offeringId)
            if offering_org_uuid:
                graph.add((offering_uri, ELM.providedBy, URIRef(f"urn:uuid:{offering_org_uuid}")))

            self._add_identifier_entry(graph, offering_uri, offering.get("primaryCode"))
            for other in (offering.get("otherCodes") or []):
                self._add_identifier_entry(graph, offering_uri, other)

            if offering.get("title"):
                title = self.extract_english_value(offering.get("title"))
                if title:
                    graph.add((offering_uri, DCTERMS.title, Literal(title, lang="en")))
            if offering.get("description"):
                description = self.extract_english_value(offering.get("description"))
                if description:
                    graph.add((offering_uri, DCTERMS.description, Literal(description, lang="en")))

            if offering.get("teachingLanguage"):
                lang_uri = language_tag_to_uri(offering.get("teachingLanguage"))
                if isinstance(lang_uri, URIRef):
                    graph.add((offering_uri, DCTERMS.language, lang_uri))

            if offering.get("startDate") or offering.get("endDate") or offering.get("academicSessionCode"):
                temporal = BNode()
                graph.add((temporal, RDF.type, DCTERMS.PeriodOfTime))
                if offering.get("startDate"):
                    graph.add((temporal, ELM.startDate, Literal(offering.get("startDate"), datatype=get_date_datatype(offering.get("startDate")))))
                if offering.get("endDate"):
                    graph.add((temporal, ELM.endDate, Literal(offering.get("endDate"), datatype=get_date_datatype(offering.get("endDate")))))
                if offering.get("academicSessionCode"):
                    graph.add((temporal, SKOS.prefLabel, Literal(offering.get("academicSessionCode"))))
                graph.add((offering_uri, DCTERMS.temporal, temporal))

            self._value_to_concept(offering, "offeringFormat", graph, offering_uri, ELM.mode, self.MODE_MAP)

            self._value_to_literal(offering, "maxNumberStudents",       graph, offering_uri, QL.enrolmentCapacity,      datatype=XSD.nonNegativeInteger)
            self._value_to_literal(offering, "enrolledNumberStudents",  graph, offering_uri, QL.enrolledLearnerCount,   datatype=XSD.nonNegativeInteger)
            self._value_to_literal(offering, "minNumberStudents",       graph, offering_uri, QL.enrolmentMinimum,       datatype=XSD.nonNegativeInteger)

            # locations[] → elm:location (description only; geo coords ignored —
            # no ELM property defined on Location for them)
            for loc_entry in (offering.get("locations") or []):
                if not isinstance(loc_entry, dict):
                    continue
                loc = BNode()
                graph.add((loc, RDF.type, DCTERMS.Location))
                if name := self.extract_english_value(loc_entry.get("description")):
                    graph.add((loc, ELM.geographicName, Literal(name, lang="en")))
                if ident := loc_entry.get("identifier"):
                    self._add_identifier(graph, loc, ident)
                graph.add((offering_uri, ELM.location, loc))

            # roleEnablement[] with role=student → elm:applicationDeadline (endDate of the student role)
            for role in (offering.get("roleEnablement") or []):
                if not isinstance(role, dict) or role.get("role") != "student":
                    continue
                if end := role.get("endDate"):
                    graph.add((offering_uri, ELM.applicationDeadline, Literal(end, datatype=get_date_datatype(end))))

            # paceOfStudy → elm:learningSchedule (skipped if URI constants are unset)
            if schedule_uri := _pace_to_schedule(offering.get("paceOfStudy")):
                graph.add((offering_uri, ELM.learningSchedule, schedule_uri))

            self._add_record_status(graph, offering_uri, offering.get("recordStatus"))

            if dlm := offering.get("dateLastModified"):
                graph.add((offering_uri, DCTERMS.modified, Literal(dlm, datatype=XSD.dateTime)))

        return course_uuid
