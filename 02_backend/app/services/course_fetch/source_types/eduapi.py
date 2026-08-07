import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from rdflib import BNode, Graph, Literal, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, SKOS, XSD

from services.vocabulary import language_tag_to_uri

from .base import (
    ADMS,
    DataSourceType,
    ELM,
    QL,
    add_inverse_part_links,
    isced_f_code_to_uri,
    org_uuid_from_value,
    get_date_datatype,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Learning-schedule concepts
#
# The EU snb `learning-schedule` vocabulary defines exactly 3 concepts, all
# mapped below. Verified against
# http://data.europa.eu/snb/learning-schedule/25831c2.
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

    MODE_ONLINE = URIRef("http://data.europa.eu/snb/learning-assessment/920fbb3cbe") # Online
    MODE_MAP = {
        "online":   MODE_ONLINE, # Online
        "blended":  URIRef("http://data.europa.eu/snb/learning-assessment/c_3a90b26d"), # Hybrid
        "onGround": URIRef("http://data.europa.eu/snb/learning-assessment/9191af2ed9"), # Presential
    }

    # Only mappings with a confirmed EU snb learning-opportunity concept are
    # listed here. Other courseType/offeringType values (standard, honors,
    # research, independentStudy, practicum, studyAbroad, capstone, clinical,
    # correspondence, fieldExperience, seminar) fall through to COURSE_TYPE.
    # The concept URIs are opaque hex identifiers that need to be looked up
    # from the EU snb learning-opportunity SKOS scheme once available.
    COURSE_TYPE_MAP = {
        "internship": URIRef("http://data.europa.eu/snb/learning-opportunity/77b99de990"),
        "thesis":     URIRef("http://data.europa.eu/snb/learning-opportunity/b2434ca358"),
    }

    # collectionType → EU learning-opportunity concept, on the same
    # standalone-programme vs. component-of-a-programme axis as the OOAPI
    # adapter's PROGRAMME_TYPE_MAP. Everything but `program` is a grouping
    # *within* a programme of study, and "Programme module" is the only EU
    # concept for a part of a programme.
    #
    # `nonDegreeCollection` deliberately falls back to PROGRAMME_TYPE rather
    # than mapping to "Short learning programme" — non-degree does not imply
    # short. `ext:`-prefixed custom values fall back too.
    #
    # No elm:specialisationOf is inferred from programSpecialization: Edu-API
    # has no canonical way to express specialisation, and a specialisation
    # nested under a parent is not reliably a specialisation *of that parent*.
    COLLECTION_TYPE_MAP = {
        "program":               DataSourceType.PROGRAMME_TYPE,
        "generalEducation":      DataSourceType.PROGRAMME_MODULE_TYPE,
        "requiredCollection":    DataSourceType.PROGRAMME_MODULE_TYPE,
        "electiveCollection":    DataSourceType.PROGRAMME_MODULE_TYPE,
        "capstoneCollection":    DataSourceType.PROGRAMME_MODULE_TYPE,
        "majorCollection":       DataSourceType.PROGRAMME_MODULE_TYPE,
        "minorCollection":       DataSourceType.PROGRAMME_MODULE_TYPE,
        "programSpecialization": DataSourceType.PROGRAMME_MODULE_TYPE,
    }

    def _fetch_all(self, session, url: str, *, tolerate_errors: bool = False) -> List[Dict]:
        """Fetch every item from an Edu-API collection endpoint.

        Edu-API returns a bare JSON array with no envelope, so the walk pages on
        limit/offset and stops on the first short page. Each call builds its own
        params so a leftover offset cannot leak between endpoints.

        With `tolerate_errors`, a non-200 yields an empty list instead of
        raising — used for endpoints a provider may not implement.
        """
        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        limit = self.source.get("pageSize", 500)
        params["limit"] = limit
        params["offset"] = 0

        logger.info("Edu-API request to %s", url)

        items: List[Dict] = []
        while True:
            response = session.get(url, params=params, timeout=60)
            if tolerate_errors and response.status_code != requests.codes.OK:
                if params["offset"] == 0:
                    logger.info(
                        "Edu-API %s returned %s — skipping this collection",
                        url, response.status_code,
                    )
                return items
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list):
                logger.warning("Edu-API %s returned a non-list payload — stopping", url)
                return items
            logger.info("Edu-API page: %s item(s)", len(page))
            items.extend(page)
            if len(page) < limit:
                return items
            params["offset"] += limit

    def _group_offerings(
        self, offerings: List[Dict], parents: Dict, fk: str, label: str,
    ) -> Dict[str, List[Dict]]:
        """Bucket offerings by the template they belong to, warning on orphans."""
        grouped: Dict[str, List[Dict]] = {key: [] for key in parents}
        for offering in offerings:
            parent_id = offering.get(fk)
            if parent_id in grouped:
                grouped[parent_id].append(offering)
            else:
                logger.warning(
                    "- %s %s refers to unknown %s %s",
                    label, offering.get("sourcedId"), fk, parent_id,
                )
        return grouped

    def _do_fetch(self, session):
        base = self.source["path"]

        courses = {
            c["sourcedId"]: c
            for c in self._fetch_all(session, urljoin(base, "courseTemplates"))
            if c.get("sourcedId")
        }
        course_offerings = self._group_offerings(
            self._fetch_all(session, urljoin(base, "courseOfferings")),
            courses, "course", "courseOffering",
        )

        # Collections are tolerated as missing: not every Edu-API provider
        # exposes collectionTemplates, and a 404 must not fail a good course
        # fetch.
        collections = {
            c["sourcedId"]: c
            for c in self._fetch_all(
                session, urljoin(base, "collectionTemplates"), tolerate_errors=True
            )
            if c.get("sourcedId")
        }
        collection_offerings = self._group_offerings(
            self._fetch_all(
                session, urljoin(base, "collectionOfferings"), tolerate_errors=True
            ),
            collections, "collection", "collectionOffering",
        )

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        los_subjects: set = set()
        courses_ok = courses_failed = 0
        collections_ok = collections_failed = 0

        for course_id, course in courses.items():
            if self.map_course_to_rdf(course, graph, course_offerings[course_id]):
                courses_ok += 1
                los_subjects.add(self._get_uri(course_id))
            else:
                courses_failed += 1

        for collection_id, collection in collections.items():
            if self.map_collection_to_rdf(
                collection, graph, collection_offerings[collection_id]
            ):
                collections_ok += 1
                los_subjects.add(self._get_uri(collection_id))
            else:
                collections_failed += 1

        dangling = add_inverse_part_links(graph, los_subjects)
        if dangling:
            logger.info(
                "Edu-API: %s part link(s) reference a learning opportunity this "
                "source does not publish; left as a one-way reference",
                dangling,
            )

        logger.info(
            "Edu-API fetch done: %s courses ok, %s collections ok, %s failed, %s triples",
            courses_ok, collections_ok, courses_failed + collections_failed, len(graph),
        )
        return graph.serialize(format="turtle", encoding="utf-8"), "text/turtle"

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

    def _new_los(self, graph: Graph, source_id: str):
        """Create the shared LOS skeleton for a course or a collection.

        Returns `(uri, uuid)`. dcterms:type is left to the caller — in the
        QualityLink profile a course and a programme are the same class, and
        only the type distinguishes them.
        """
        uri = self._get_uri(source_id)
        los_uuid = self._get_uuid(source_id, uri)
        graph.add((uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((uri, QL.sourceType, QL.EduApiSource))
        graph.add((URIRef(f"urn:uuid:{los_uuid}"), OWL.sameAs, uri))
        return uri, los_uuid

    def _map_los_common(self, source: Dict, graph: Graph, subject: URIRef) -> None:
        """Map the fields Edu-API CourseTemplate and CollectionTemplate share.

        The two schemas differ by exactly one property (`courseType` vs
        `collectionType`, handled by the callers), so everything else lives here.
        """
        source_id = source.get("sourcedId")

        # owning organisation
        org_uuid = org_uuid_from_value(source.get("organization"), owner=source_id)
        if org_uuid:
            graph.add((subject, DCTERMS.publisher, URIRef(f"urn:uuid:{org_uuid}")))

        # identifiers
        self._add_identifier_entry(graph, subject, source.get("primaryCode"))
        for other in (source.get("otherCodes") or []):
            self._add_identifier_entry(graph, subject, other)

        if source.get("title"):
            title = self.extract_english_value(source.get("title"))
            if title:
                graph.add((subject, DCTERMS.title, Literal(title, lang="en")))

        if source.get("description"):
            description = self.extract_english_value(source.get("description"))
            if description:
                graph.add((subject, DCTERMS.description, Literal(description, lang="en")))

        self._value_to_concept(source, "level", graph, subject, ELM.EQFLevel, self.LEVEL_MAP)

        # subjectCodes[]: ISCED-F where it looks like a numeric ISCED code
        # (4-digit codes pass through; 5–6 digit SOI2021 codes auto-truncate
        # to the parent 4-digit ISCED concept), otherwise educationSubject.
        for code in (source.get("subjectCodes") or []):
            if not isinstance(code, str) or not code:
                continue
            if uri := isced_f_code_to_uri(code):
                graph.add((subject, ELM.ISCEDFCode, uri))
            else:
                subj = BNode()
                graph.add((subj, RDF.type, SKOS.Concept))
                graph.add((subj, SKOS.notation, Literal(code)))
                graph.add((subject, ELM.educationSubject, subj))

        # try to parse credits
        if source.get("creditType") == "credit" and "creditsAwarded" in source:
            if m := re.match(r"\d+(\.\d+)?", source["creditsAwarded"]):
                credit = BNode()
                graph.add((credit, ELM.point, Literal(m[0], datatype=XSD.double)))
                if re.search("ECTS", source["creditsAwarded"], re.IGNORECASE):
                    graph.add((credit, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((subject, ELM.creditPoint, credit))

        if source.get("teachingLanguage"):
            lang_uri = language_tag_to_uri(source.get("teachingLanguage"))
            if isinstance(lang_uri, URIRef):
                graph.add((subject, DCTERMS.language, lang_uri))

        # gradingScheme[] → elm:gradingScheme (single blank node, English title)
        if scheme_title := self.extract_english_value(source.get("gradingScheme")):
            scheme = BNode()
            graph.add((scheme, RDF.type, ELM.GradingScheme))
            graph.add((scheme, DCTERMS.title, Literal(scheme_title, lang="en")))
            graph.add((subject, ELM.gradingScheme, scheme))

        # recordStatus → ql:isActive
        self._add_record_status(graph, subject, source.get("recordStatus"))

        # dateLastModified → dcterms:modified
        if dlm := source.get("dateLastModified"):
            graph.add((subject, DCTERMS.modified, Literal(dlm, datatype=XSD.dateTime)))

        # parent[] → elm:isPartOf, URI shared with the parent's own resource.
        # On a CourseTemplate these are EducationCollection refs; on a
        # CollectionTemplate they are the enclosing collection. Edu-API only
        # ever expresses the relation upward, so the reverse elm:hasPart is
        # materialised once per fetch by add_inverse_part_links.
        for parent_id in (source.get("parent") or []):
            if isinstance(parent_id, str) and parent_id:
                graph.add((subject, ELM.isPartOf, self._get_uri(parent_id)))

    def map_course_to_rdf(self, course: Dict, graph: Graph, offerings: List):
        courseId = course.get("sourcedId")
        if not courseId:
            return None

        course_uri, course_uuid = self._new_los(graph, courseId)
        self._add_type(
            graph, course_uri, course.get("courseType"),
            self.COURSE_TYPE_MAP, self.COURSE_TYPE,
        )

        self._map_los_common(course, graph, course_uri)

        for offering in offerings:
            self._map_offering(offering, graph, course_uri, self.COURSE_TYPE_MAP)

        return course_uuid

    def map_collection_to_rdf(self, collection: Dict, graph: Graph, offerings: List):
        """Map an Edu-API CollectionTemplate (an EducationCollection) to a LOS.

        `collectionType == "program"` is the programme-like case; the other
        values are groupings within a programme. Either way this is the same
        class as a course, differing only in dcterms:type.
        """
        collectionId = collection.get("sourcedId")
        if not collectionId:
            return None

        collection_uri, collection_uuid = self._new_los(graph, collectionId)
        self._add_type(
            graph, collection_uri, collection.get("collectionType"),
            self.COLLECTION_TYPE_MAP, self.PROGRAMME_TYPE,
        )

        self._map_los_common(collection, graph, collection_uri)

        for offering in offerings:
            self._map_offering(offering, graph, collection_uri, self.COLLECTION_TYPE_MAP)

        return collection_uuid

    def _map_offering(
        self, offering: Dict, graph: Graph, parent_uri: URIRef, type_map: Dict,
    ) -> None:
        """Map one offering to a LOS instance hanging off `parent_uri`.

        Serves both CourseOffering and CollectionOffering: the two schemas differ
        only in the FK naming the template they belong to, which the caller has
        already resolved. `type_map` is the enum map for `offeringType`, which
        follows the parent's enum (courseType vs collectionType).
        """
        offeringId = offering.get("sourcedId")
        offering_uri = URIRef(
            f"{parent_uri}/offerings/{offeringId}"
        )
        graph.add((offering_uri, RDF.type, QL.LearningOpportunityInstance))
        graph.add((offering_uri, ELM.learningAchievementSpecification, parent_uri))

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

        # Temporal: prefer offering's own start/end; else fall back to the
        # linked academicSession's dates/title. academicSessionCode is used
        # as a short label; the session's `title[]` (if present) enriches
        # skos:prefLabel per language when the code alone is unhelpful.
        session = offering.get("academicSession") if isinstance(offering.get("academicSession"), dict) else None
        off_start = offering.get("startDate")
        off_end = offering.get("endDate")
        session_start = session.get("startDate") if session else None
        session_end = session.get("endDate") if session else None
        session_title = self.extract_english_value(session.get("title")) if session else ""
        code = offering.get("academicSessionCode")

        if off_start or off_end or session_start or session_end or code or session_title:
            temporal = BNode()
            graph.add((temporal, RDF.type, DCTERMS.PeriodOfTime))
            if effective_start := off_start or session_start:
                graph.add((temporal, ELM.startDate, Literal(effective_start, datatype=get_date_datatype(effective_start))))
            if effective_end := off_end or session_end:
                graph.add((temporal, ELM.endDate, Literal(effective_end, datatype=get_date_datatype(effective_end))))
            if code:
                graph.add((temporal, SKOS.prefLabel, Literal(code)))
            if session_title:
                graph.add((temporal, SKOS.prefLabel, Literal(session_title, lang="en")))
            graph.add((offering_uri, DCTERMS.temporal, temporal))

        # offeringType (the parent's enum) → dcterms:type on the LOI.
        # LOS keeps its own type; the LOI type reflects the delivery form.
        if offering.get("offeringType") in type_map:
            graph.add((offering_uri, DCTERMS.type, type_map[offering["offeringType"]]))

        self._value_to_concept(offering, "offeringFormat", graph, offering_uri, ELM.mode, self.MODE_MAP)

        # synchronicity refines elm:mode: fully-async delivery reads as "online",
        # unless offeringFormat is already defined. Other values (synchronous, hybrid) add
        # no signal beyond what offeringFormat conveys.
        if offering.get("synchronicity") == "asynchronous" and (offering_uri, ELM.mode, None) not in graph:
            graph.add((offering_uri, ELM.mode, self.MODE_ONLINE))

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
