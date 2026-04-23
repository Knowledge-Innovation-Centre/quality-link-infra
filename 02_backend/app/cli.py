from typing import List, Optional, Annotated
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table

from config import DEFAULT_VOCABULARIES, DEQAR_API_URL
from database import SessionLocal
from services.course_fetch.main import run_course_fetch
from services.courses import (
    list_provider_courses,
    frame_course,
    resolve_course_uri,
    resolve_course_uuid,
    CourseNotFound,
)
from services.datalake import queue_provider_data
from services.deqar import (
    fetch_deqar_providers,
    providers_to_rdf,
    push_providers_to_fuseki,
    upsert_providers,
)
from services.manifest import refresh_manifest_for_provider
from services.providers import (
    get_provider,
    list_providers,
    resolve_provider_uuid,
)
from services.vocabulary import refresh_vocabulary

app = typer.Typer(help="QualityLink pipeline admin CLI", no_args_is_help=True)

providers_app = typer.Typer(help="Provider and data source operations", no_args_is_help=True)
app.add_typer(providers_app, name="provider")

vocabularies_app = typer.Typer(help="EU controlled vocabulary operations", no_args_is_help=True)
app.add_typer(vocabularies_app, name="vocabulary")

courses_app = typer.Typer(help="Courses operations", no_args_is_help=True)
app.add_typer(courses_app, name="course")

console = Console()


def _die(message: str, code: int = 1) -> None:
    console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=code)


def _resolve(db, value: str) -> UUID:
    try:
        return resolve_provider_uuid(db, value)
    except HTTPException as e:
        _die(str(e.detail))


def _render_manifest_probes(probes: Optional[list]) -> Table:
    table = Table(title="Probes tried")
    table.add_column("Domain")
    table.add_column("Type")
    table.add_column("Result")
    table.add_column("Path")
    for probe in probes or []:
        check = probe.get("check")
        if check is True:
            cell = "[green]hit[/green]"
        elif check is False:
            cell = "[red]miss[/red]"
        else:
            cell = "[dim]skipped[/dim]"
        table.add_row(
            probe.get("domain") or "-",
            probe.get("type") or "-",
            cell,
            probe.get("path") or "-",
        )
    return table


@providers_app.command("list")
def providers_list(
    search: str = typer.Argument(
        default=None,
        help="Filter by name, ETER id, or DEQAR id"
    ),
    with_data: bool = typer.Option(
        False,
        "--with-data", "-d",
        help="Only providers for which a manifest and data sources were found",
    ),
    page: int = typer.Option(1, "--page", "-p", min=1),
    page_size: int = typer.Option(20, "--page-size", min=1, max=200),
) -> None:
    """
    List or search providers.
    """

    with SessionLocal() as db:
        result = list_providers(
            db, search=search, with_data=with_data, page=page, page_size=page_size
        )

    total = result["total"]
    total_pages = max(1, result["total_pages"])
    table = Table(
        title=f"Providers — {total} match{'es' if total != 1 else ''} (page {page}/{total_pages})"
    )
    table.add_column("UUID", no_wrap=True)
    table.add_column("ETER ID")
    table.add_column("DEQAR ID")
    table.add_column("Name")
    table.add_column("Last manifest pull")
    table.add_column("Sources")

    for row in result["response"]:
        pulled = row["last_manifest_pull"]
        table.add_row(
            row["provider_uuid"],
            row["eter_id"] or "-",
            row["deqar_id"] or "-",
            row["provider_name"] or "-",
            pulled[:16].replace("T", " ") if pulled else "-",
            "[green]yes[/green]" if row["has_sources"] else "[dim]no[/dim]",
        )
    console.print(table)


@providers_app.command("manifest")
def providers_refresh_manifest(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """
    Run manifest discovery for a provider (DNS TXT + .well-known).
    """

    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)

        try:
            with console.status(f"Pulling manifest for {provider_uuid}..."):
                result = refresh_manifest_for_provider(db, provider_uuid)
        except HTTPException as e:
            _die(str(e.detail))

    if result.get("status") == "busy":
        console.print(f"[yellow]{result.get('message')}[/yellow]")
        raise typer.Exit(code=2)

    found = result.get("manifest_found")
    if found:
        console.print(
            f"Manifest found: [green]yes[/green] — {result.get('manifest_url')}"
        )
    else:
        console.print("Manifest found: [red]no[/red]")
    console.print(f"Sources processed: {result.get('sources_processed')}")
    console.print(
        f"New source version created: {result.get('new_source_version_created')}"
    )

    console.print(_render_manifest_probes(result.get("manifest_json")))


@providers_app.command("sources")
def providers_list_sources(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """
    List data sources of a provider's latest manifest version, plus the
    manifest probes recorded during the last discovery run.
    """

    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        try:
            result = get_provider(db, provider_uuid)
        except HTTPException as e:
            _die(str(e.detail))

    provider_row = result.get("provider") or {}
    pulled = provider_row.get("last_manifest_pull")
    probes = provider_row.get("manifest_json")

    if probes:
        console.print(
            f"Last manifest pull: [cyan]{pulled[:16].replace('T', ' ') if pulled else '-'}[/cyan]"
        )
        hit = next((p for p in probes if p.get("check") is True), None)
        if hit:
            console.print(
                f"Manifest found: [green]yes[/green] — {hit.get('path') or '-'}"
            )
        else:
            console.print("Manifest found: [red]no[/red]")
        console.print(_render_manifest_probes(probes))
    else:
        console.print(
            "[yellow]No manifest discovery has been run for this provider yet.[/yellow]"
        )

    version = result.get("source_version")
    sources = result.get("sources") or []
    if not version:
        console.print(f"[yellow]No source version found for {provider_uuid}[/yellow]")
        raise typer.Exit(code=2)

    console.print(
        f"Latest version: [cyan]{version['source_version_uuid']}[/cyan] "
        f"({version['version_date']} / v{version['version_id']})"
    )

    if not sources:
        console.print("[yellow]No sources attached to this version.[/yellow]")
        return

    table = Table(title=f"Sources — {len(sources)}")
    table.add_column("Source UUID", no_wrap=True)
    table.add_column("Name")
    table.add_column("Type")
    table.add_column("Version")
    table.add_column("Path")
    table.add_column("Last file pushed")
    for s in sources:
        pushed = s.get("last_file_pushed_date")
        table.add_row(
            s["source_uuid"],
            s.get("source_name") or "-",
            s.get("source_type") or "-",
            s.get("source_version") or "-",
            s.get("source_path") or "-",
            pushed[:16].replace("T", " ") if pushed else "-",
        )
    console.print(table)


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
    table.add_column("Course UUID", no_wrap=True)
    table.add_column("Type")
    table.add_column("Title")
    table.add_column("URI")
    for c in courses:
        title = c["title"] or ""
        if title and c.get("title_lang"):
            title = f"{title} [dim]({c['title_lang']})[/dim]"
        table.add_row(
            c["course_uuid"] or "-",
            c["type"] or "-",
            title or "[dim]—[/dim]",
            c["uri"] or "-",
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


@providers_app.command("fetch")
def providers_fetch(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
    source_uuid: Optional[UUID] = typer.Option(
        None,
        "--source", "-s",
        help="Fetch a single source; default fetches every source of the latest version",
    ),
) -> None:
    """Fetch provider data in-process (bronze → silver → gold)."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        try:
            result = get_provider(db, provider_uuid)
        except HTTPException as e:
            _die(str(e.detail))

        version = result.get("source_version")
        sources = result.get("sources") or []
        if not version:
            _die(f"No source version found for {provider_uuid}")

        version_uuid = UUID(version["source_version_uuid"])

        if source_uuid is not None:
            sources = [s for s in sources if UUID(s["source_uuid"]) == source_uuid]
            if not sources:
                _die(f"Source {source_uuid} does not belong to the latest version")

        if not sources:
            _die("No sources attached to the latest version")

        validated = []
        for s in sources:
            try:
                # Validate only — do not schedule a BackgroundTask from the CLI.
                check = queue_provider_data(
                    db,
                    provider_uuid,
                    version_uuid,
                    UUID(s["source_uuid"]),
                )
            except HTTPException as e:
                console.print(
                    f"[red]{s.get('source_name') or s['source_uuid']}: {e.detail}[/red]"
                )
                continue

            label = s.get("source_name") or s["source_uuid"]
            status_ = check.get("status")
            if status_ == "success":
                validated.append((s, label))
            elif status_ == "busy":
                console.print(f"[yellow]busy[/yellow] {label}: {check.get('message')}")
            elif status_ == "outdated":
                console.print(f"[yellow]outdated[/yellow] {label}: {check.get('message')}")
            else:
                console.print(f"[red]{label}: {check}[/red]")

    fetched = 0
    for s, label in validated:
        console.print(f"[cyan]fetching[/cyan] {label} ({s['source_uuid']})...")
        run_course_fetch(
            provider_uuid, version_uuid, UUID(s["source_uuid"])
        )
        fetched += 1

    console.print(f"\nFetched {fetched}/{len(sources)} source(s).")


@providers_app.command("refresh")
def providers_refresh_from_deqar(
    limit: int = typer.Option(2000, "--limit", min=1, help="Page size"),
    offset: int = typer.Option(0, "--offset", min=0, help="Initial page offset"),
    api_url: str = typer.Option(DEQAR_API_URL, "--api-url", help="DEQAR providers endpoint"),
) -> None:
    """
    Refresh provider registry from the DEQAR API and push to the reference graph.
    """

    with console.status("Fetching DEQAR providers..."):
        providers = fetch_deqar_providers(limit=limit, offset=offset, api_url=api_url)

    if not providers:
        console.print("[yellow]No providers returned from DEQAR[/yellow]")
        raise typer.Exit(code=2)

    with SessionLocal() as db:
        with console.status(f"Upserting {len(providers)} providers into Postgres..."):
            stats = upsert_providers(db, providers)

    with console.status("Serializing providers to RDF..."):
        rdf_list = providers_to_rdf(stats)

    with console.status(f"Pushing {len(rdf_list)} providers to Fuseki reference graph..."):
        push_stats = push_providers_to_fuseki(rdf_list)

    table = Table(title="DEQAR refresh summary")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Total processed", str(stats.total))
    table.add_row("New", str(stats.new))
    table.add_row("Updated", str(stats.updated))
    table.add_row("Unchanged", str(stats.unchanged))
    table.add_row("DB errors", str(stats.errors))
    table.add_row("Fuseki success", str(push_stats.success))
    table.add_row("Fuseki failed", str(push_stats.failed))
    console.print(table)


@vocabularies_app.command("fetch")
def vocabularies_fetch(
    schemes: List[str] = typer.Argument(
        default=None,
        help="Scheme URIs to fetch; repeat for multiple. Default: fetch DEFAULT_VOCABULARIES from config.",
    ),
) -> None:
    """
    Fetch controlled vocabularies from EU Publications and push to Fuseki.
    """
    if not schemes:
        schemes = DEFAULT_VOCABULARIES

    table = Table(title=f"Vocabulary fetch — {len(schemes)} scheme(s)")
    table.add_column("Scheme URI")
    table.add_column("Concepts", justify="right")
    table.add_column("Bytes", justify="right")
    table.add_column("Result")

    any_failed = False
    for uri in schemes:
        with console.status(f"Fetching {uri}..."):
            stats = refresh_vocabulary(uri)
        result = "[green]ok[/green]" if stats.success else f"[red]{stats.error or 'failed'}[/red]"
        table.add_row(uri, str(stats.concepts), str(stats.bytes_uploaded), result)
        if not stats.success:
            any_failed = True

    console.print(table)
    if any_failed:
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
