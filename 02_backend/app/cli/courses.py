from typing import Optional
from uuid import UUID

import requests
import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from config import GRAPH_COURSES
from database import SessionLocal
from services import fuseki
from services.course_fetch.bronze import (
    latest_bronze_for_source,
    list_sources_with_bronze,
)
from services.course_fetch.gold import list_all_courses, reindex_course
from services.course_fetch.main import run_course_fetch, run_silver_only
from services.courses import (
    CourseNotFound,
    frame_course,
    list_provider_courses,
    resolve_course_uri,
    resolve_course_uuid,
)
from services.datalake import queue_provider_data
from services.providers import get_provider, resolve_provider_uuid

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
def courses_list(
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
def courses_frame(
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


@courses_app.command("fetch")
def courses_fetch(
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


@courses_app.command("silver")
def courses_silver(
    provider: Optional[str] = typer.Argument(
        None, help="Provider UUID, ETER id, or DEQAR id (omit with --source or --all)"
    ),
    source_uuid: Optional[UUID] = typer.Option(
        None, "--source", "-s", help="Re-silver a single source by UUID",
    ),
    all_: bool = typer.Option(
        False, "--all", help="Re-silver every source that has a bronze file",
    ),
    no_reindex: bool = typer.Option(
        False, "--no-reindex",
        help="Skip the Meilisearch reindex (gold) step. By default, silver is "
             "followed by a reindex of the produced courses.",
    ),
) -> None:
    """Re-run the silver stage from each source's latest bronze file on disk.

    `--all` and the provider-scoped form consider only sources attached to
    their provider's latest source_version. `--source <UUID>` is an explicit
    override and re-silvers that source regardless of version freshness.
    """
    if not any([provider, source_uuid, all_]):
        _die("Specify one of PROVIDER, --source, or --all")
    if all_ and (provider or source_uuid):
        _die("--all cannot be combined with PROVIDER or --source")

    with SessionLocal() as db:
        if source_uuid is not None:
            message = latest_bronze_for_source(db, source_uuid)
            if not message:
                _die(f"Source {source_uuid} has no bronze file on record")
            if provider is not None:
                provider_uuid = _resolve(db, provider)
                if UUID(message["provider_uuid"]) != provider_uuid:
                    _die(
                        f"Source {source_uuid} does not belong to provider "
                        f"{provider_uuid}"
                    )
            # Resolve source_name for the display label.
            name_row = db.execute(
                text("SELECT source_name, source_type FROM source WHERE source_uuid = :s"),
                {"s": str(source_uuid)},
            ).fetchone()
            targets = [{
                "source_uuid": str(source_uuid),
                "provider_uuid": message["provider_uuid"],
                "source_version_uuid": message["source_version_uuid"],
                "source_name": name_row[0] if name_row else None,
                "source_type": (name_row[1] if name_row else None) or message.get("source_type"),
            }]
        elif all_:
            targets = list_sources_with_bronze(db)
        else:
            provider_uuid = _resolve(db, provider)
            targets = list_sources_with_bronze(db, provider_uuid=provider_uuid)

    if not targets:
        console.print("[yellow]No matching sources with a bronze file on record.[/yellow]")
        raise typer.Exit(code=2)

    reindex = not no_reindex
    suffix = "" if reindex else " (no reindex)"
    table = Table(title=f"Silver re-run{suffix} — {len(targets)} source(s)")
    table.add_column("Source")
    table.add_column("Type")
    table.add_column("Status")
    table.add_column("Courses", justify="right")
    table.add_column("Uploaded", justify="right")
    if reindex:
        table.add_column("Reindexed", justify="right")
    table.add_column("Bronze file")

    succeeded = 0
    course_total = 0
    uploaded_total = 0
    reindex_uploaded_total = 0
    reindex_failed_total = 0
    for t in targets:
        label = t.get("source_name") or t["source_uuid"]
        console.print(f"[cyan]silver[/cyan] {label} ({t['source_uuid']})...")
        res = run_silver_only(UUID(t["source_uuid"]), reindex=reindex)
        if res["status"] == "success":
            succeeded += 1
            status_cell = "[green]success[/green]"
        elif res["status"] == "busy":
            status_cell = "[yellow]busy[/yellow]"
        else:
            msg = res.get("error") or "failed"
            status_cell = f"[red]{msg}[/red]"
        course_count = res.get("course_count") or 0
        uploaded = res.get("uploaded_count") or 0
        course_total += course_count
        uploaded_total += uploaded
        upload_cell = str(uploaded)
        if uploaded < course_count:
            upload_cell = f"{uploaded} [red]({course_count - uploaded} failed)[/red]"
        row = [
            label,
            t.get("source_type") or "-",
            status_cell,
            str(course_count),
            upload_cell,
        ]
        if reindex:
            reindexed = res.get("reindex_uploaded") or 0
            reindex_failed = res.get("reindex_failed") or 0
            reindex_uploaded_total += reindexed
            reindex_failed_total += reindex_failed
            cell = str(reindexed)
            if reindex_failed:
                cell = f"{reindexed} [red]({reindex_failed} failed)[/red]"
            row.append(cell)
        row.append(res.get("bronze_file_path") or "-")
        table.add_row(*row)

    console.print(table)
    console.print(f"\nRe-silvered {succeeded}/{len(targets)} source(s).")
    upload_msg = f"Uploaded {uploaded_total}/{course_total} course(s) to Fuseki."
    if uploaded_total < course_total:
        upload_msg += f" [red]{course_total - uploaded_total} failed.[/red]"
    console.print(upload_msg)
    if reindex:
        msg = f"Reindexed {reindex_uploaded_total} course(s)."
        if reindex_failed_total:
            msg += f" [red]{reindex_failed_total} failed.[/red]"
        console.print(msg)


@courses_app.command("rebuild-graph")
def courses_rebuild_graph(
    yes: bool = typer.Option(
        False, "--yes", help="Confirm clearing and rebuilding the courses graph",
    ),
    no_reindex: bool = typer.Option(
        False, "--no-reindex", help="Skip the final Meilisearch reindex",
    ),
) -> None:
    """Clear the Fuseki courses graph and rebuild it from the retained bronze files.

    Silver used to copy a referenced learning opportunity's triples into every
    referring subject's payload. replace_subject_in_graph only deletes triples
    belonging to the subject being pushed, and INSERT DATA relabels blank nodes
    on each request, so those copies accumulated a duplicate blank-node subtree
    per run and were never cleaned up. Bounding the traversal stops new damage;
    this command removes what earlier runs already wrote.

    A surgical DELETE is not viable — the stale set is defined by reachability
    under the new traversal, and a duplicated blank-node subtree is
    indistinguishable from a legitimate one without re-deriving the subgraph.
    Rebuilding is safe because course UUIDs are derived deterministically
    (uuid5 over the subject URI), so no Meilisearch document id changes.

    Only the courses graph is touched; the reference and vocabulary graphs are
    left alone.
    """
    if not yes:
        _die(
            "This clears the whole courses graph and re-runs silver for every "
            "source with a bronze file. Re-run with --yes to confirm."
        )

    before = fuseki.count_graph_triples(GRAPH_COURSES)
    console.print(
        f"Courses graph: [cyan]{GRAPH_COURSES}[/cyan] — "
        f"{before if before is not None else '?'} triples before"
    )

    with SessionLocal() as db:
        targets = list_sources_with_bronze(db)
    if not targets:
        _die("No sources with a bronze file on record — refusing to clear the graph.")

    console.print(f"Found [cyan]{len(targets)}[/cyan] source(s) with a bronze file.")

    with console.status("Clearing courses graph..."):
        if not fuseki.sparql_update(f"CLEAR GRAPH <{GRAPH_COURSES}>"):
            _die("CLEAR GRAPH failed — see logs. Nothing was rebuilt.")
    console.print("[green]cleared[/green]")

    succeeded = 0
    failed: list[str] = []
    course_total = 0
    uploaded_total = 0
    for t in targets:
        label = t.get("source_name") or t["source_uuid"]
        console.print(f"[cyan]silver[/cyan] {label} ({t['source_uuid']})...")
        # Reindex once at the end instead of per source.
        res = run_silver_only(UUID(t["source_uuid"]), reindex=False)
        course_total += res.get("course_count") or 0
        uploaded_total += res.get("uploaded_count") or 0
        if res["status"] == "success":
            succeeded += 1
        else:
            failed.append(f"{label}: {res.get('error') or res['status']}")

    after = fuseki.count_graph_triples(GRAPH_COURSES)
    summary = Table(title="Rebuild summary")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Sources re-silvered", f"{succeeded}/{len(targets)}")
    summary.add_row("Courses uploaded", f"{uploaded_total}/{course_total}")
    summary.add_row("Triples before", str(before) if before is not None else "?")
    summary.add_row("Triples after", str(after) if after is not None else "?")
    if before is not None and after is not None:
        summary.add_row("Difference", f"{after - before:+d}")
    console.print(summary)

    for f in failed:
        console.print(f"[red]failed[/red] {f}")

    if not no_reindex:
        console.print("\nReindexing every course in Meilisearch...")
        with console.status("Enumerating all courses in Fuseki..."):
            courses = list_all_courses()
        uploaded = 0
        reindex_failed = 0
        with requests.Session() as http:
            with console.status(f"Reindexing {len(courses)} course(s)...") as status_:
                for i, c in enumerate(courses, 1):
                    if reindex_course(http, c["uuid"], c.get("uri")):
                        uploaded += 1
                    else:
                        reindex_failed += 1
                    status_.update(f"Reindexing {i}/{len(courses)}...")
        msg = f"Reindexed {uploaded}/{len(courses)} course(s)."
        if reindex_failed:
            msg += f" [red]{reindex_failed} failed.[/red]"
        console.print(msg)

    if failed:
        raise typer.Exit(code=2)


@courses_app.command("reindex")
def courses_reindex(
    course: Optional[str] = typer.Argument(
        None, help="Course URI or UUID (omit with --provider or --all)"
    ),
    provider: Optional[str] = typer.Option(
        None, "--provider", "-p",
        help="Reindex every course of a provider",
    ),
    all_: bool = typer.Option(
        False, "--all", help="Reindex every course in the Fuseki courses graph",
    ),
) -> None:
    """Re-run the gold stage: reindex courses in Meilisearch."""
    scopes = [bool(course), bool(provider), all_]
    if sum(scopes) != 1:
        _die("Specify exactly one of COURSE, --provider, or --all")

    courses: list[dict] = []

    if course is not None:
        try:
            UUID(course)
            try:
                course_uri = resolve_course_uri(course)
            except CourseNotFound as e:
                _die(str(e))
            courses = [{"uuid": course, "uri": course_uri}]
        except ValueError:
            course_uri = course
            try:
                course_uuid = resolve_course_uuid(course_uri)
            except CourseNotFound as e:
                _die(str(e))
            courses = [{"uuid": course_uuid, "uri": course_uri}]
    elif provider is not None:
        with SessionLocal() as db:
            provider_uuid = _resolve(db, provider)
            try:
                with console.status(f"Enumerating courses for {provider_uuid}..."):
                    offset = 0
                    page_size = 500
                    while True:
                        result = list_provider_courses(
                            db, provider_uuid, limit=page_size, offset=offset,
                        )
                        page = result["response"]
                        if not page:
                            break
                        for c in page:
                            if c.get("course_uuid") and c.get("uri"):
                                courses.append({"uuid": c["course_uuid"], "uri": c["uri"]})
                        offset += len(page)
                        if offset >= result["total"]:
                            break
            except HTTPException as e:
                _die(str(e.detail))
    else:  # all_
        with console.status("Enumerating all courses in Fuseki..."):
            courses = list_all_courses()

    if not courses:
        console.print("[yellow]No courses to reindex.[/yellow]")
        raise typer.Exit(code=2)

    uploaded = 0
    failed = 0
    with requests.Session() as http:
        with console.status(f"Reindexing {len(courses)} course(s)...") as status_:
            for i, c in enumerate(courses, 1):
                if reindex_course(http, c["uuid"], c.get("uri")):
                    uploaded += 1
                else:
                    failed += 1
                status_.update(f"Reindexing {i}/{len(courses)}...")

    summary = Table(title="Reindex summary")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Total", str(len(courses)))
    summary.add_row("Uploaded", str(uploaded))
    summary.add_row("Failed", str(failed))
    console.print(summary)
    if failed:
        raise typer.Exit(code=2)
