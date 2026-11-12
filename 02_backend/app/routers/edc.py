from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import JSONResponse

from services.courses import CourseNotFound, resolve_course_uri
from services.edc import InstanceNotFound, build_edc_template, validate_edc_template

router = APIRouter(tags=["EDC"])


@router.get("/courses/{course_uuid}/edc-template")
def get_edc_template(
    course_uuid: UUID,
    delivery_address: Optional[List[str]] = Query(
        None,
        alias="deliveryAddress",
        description="Recipient email or wallet address; repeat for several.",
    ),
    given_name: Optional[str] = Query(None, alias="givenName"),
    family_name: Optional[str] = Query(None, alias="familyName"),
    instance: Optional[str] = Query(
        None,
        description="URI of one of the course's learning opportunity instances to include.",
    ),
):
    """A course as an upload file for the web-based EDC issuer ("Upload JSON")."""
    try:
        course_uri = resolve_course_uri(str(course_uuid))
        doc = build_edc_template(
            course_uri,
            delivery_addresses=delivery_address,
            given_name=given_name,
            family_name=family_name,
            instance_uri=instance,
        )
    except (CourseNotFound, InstanceNotFound) as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    errors = validate_edc_template(doc)
    if errors:
        # The issuer would reject the file, usually because of a gap in the
        # source data (e.g. a publisher without legal name or location).
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Course data does not yield a valid EDC credential", "errors": errors},
        )

    return JSONResponse(
        content=doc,
        headers={"Content-Disposition": f'attachment; filename="edc-template-{course_uuid}.json"'},
    )
