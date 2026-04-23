from typing import List, Optional, Annotated
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table

from database import SessionLocal
from services.course_fetch.main import run_course_fetch
from services.courses import (
    list_provider_courses,
    frame_course,
    resolve_course_uri,
    resolve_course_uuid,
    CourseNotFound,
)
from services.providers import resolve_provider_uuid

courses_app = typer.Typer(help="Courses operations", no_args_is_help=True)

console = Console()


def _die(message: str, code: int = 1) -> None:
    console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=code)


def _resolve(db, value: str) -> UUID:
    try:
        return resolve_provider_uuid(db, value)
    except HTTPException as e:
        _die(str(e.detail))


@courses_app.command("list")
def providers_list_courses(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
    limit: int = typer.Option(50, "--limit", "-n", min=1, max=500, help="Page size"),
    offset: int = typer.Option(0, "--offset", min=0, help="Number of courses to skip"),
) -> None:
    """
    List courses published by a provider (queried from the Fuseki courses graph).
    """
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        try:
            with console.status(f"Querying courses for {provider_uuid}..."):
                result = list_provider_courses(
                    db, provider_uuid, limit=limit, offset=offset
                )
        except HTTPException as e:
            _die(str(e.detail))

    courses = result["response"]
    total = result["total"]
    start = result["offset"] + 1 if courses else 0
    end = result["offset"] + len(courses)
    console.print(
        f"Provider URI: [cyan]{result['provider_uri']}[/cyan]"
    )

    if not courses:
        if total:
            console.print(
                f"[yellow]Offset {result['offset']} is past the end "
                f"({total} total).[/yellow]"
            )
        else:
            console.print("[yellow]No courses found for this provider.[/yellow]")
        return

    table = Table(
        title=f"Courses — {total} total (showing {start}–{end})"
    )
    table.add_column("URI")
    table.add_column("Type")
    table.add_column("Title")
    table.add_column("Instances")
    table.add_column("Course UUID", no_wrap=True)
    for c in courses:
        title = c["title"] or ""
        if title and c.get("title_lang"):
            title = f"{title} [dim]({c['title_lang']})[/dim]"
        table.add_row(
            c["uri"] or "-",
            c["type"] or "-",
            title or "[dim]—[/dim]",
            c["instances"] or "-",
            c["course_uuid"] or "-",
        )
    console.print(table)


@courses_app.command("frame")
def frame_courses(
    course: str = typer.Argument(..., help="Course URI or UUID"),
) -> None:
    """
    Show a single course in framed JSON-LD
    """
    try:
        UUID(course)
        course_uuid = course
        course_uri = resolve_course_uri(course)
    except ValueError:
        course_uri = course
        course_uuid = resolve_course_uuid(course_uri)

    console.print(
        f"Course URI: [cyan]{course_uri}[/cyan] [dim](UUID {course_uuid})[/dim]"
    )

    console.print_json(data=frame_course(course_uri))

