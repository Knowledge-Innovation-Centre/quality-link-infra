from typing import Optional
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table

from database import SessionLocal
from dependencies import redis_client
from services.datalake import queue_provider_data
from services.manifest import refresh_manifest_for_provider
from services.providers import (
    get_provider,
    list_providers,
    resolve_provider_uuid,
)

app = typer.Typer(help="QL-Pipeline admin CLI", no_args_is_help=True)
providers_app = typer.Typer(help="Provider operations", no_args_is_help=True)
app.add_typer(providers_app, name="providers")

console = Console()


def _die(message: str, code: int = 1) -> None:
    console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=code)


def _resolve(db, value: str) -> UUID:
    try:
        return resolve_provider_uuid(db, value)
    except HTTPException as e:
        _die(str(e.detail))


@providers_app.command("list")
def providers_list(
    search: Optional[str] = typer.Option(
        None, "--search", "-s", help="Filter by name, ETER id, or DEQAR id"
    ),
    with_data: bool = typer.Option(
        False,
        "--with-data",
        help="Only providers for which a manifest and data sources were found",
    ),
    page: int = typer.Option(1, "--page", "-p", min=1),
    page_size: int = typer.Option(20, "--page-size", min=1, max=200),
) -> None:
    """List or search providers."""
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
    table.add_column("ETER")
    table.add_column("DEQAR")
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


@providers_app.command("refresh-manifest")
def providers_refresh_manifest(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """Re-run manifest discovery for a provider (DNS TXT + .well-known)."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)

        try:
            with console.status(f"Pulling manifest for {provider_uuid}..."):
                result = refresh_manifest_for_provider(db, redis_client, provider_uuid)
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

    table = Table(title="Probes tried")
    table.add_column("Domain")
    table.add_column("Type")
    table.add_column("Result")
    table.add_column("Path")
    for probe in result.get("manifest_json") or []:
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
    console.print(table)


@providers_app.command("list-sources")
def providers_list_sources(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """List data sources of a provider's latest manifest version."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        try:
            result = get_provider(db, provider_uuid)
        except HTTPException as e:
            _die(str(e.detail))

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


@providers_app.command("fetch")
def providers_fetch(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
    source_uuid: Optional[UUID] = typer.Option(
        None,
        "--source-uuid",
        help="Queue a single source; default queues every source of the latest version",
    ),
) -> None:
    """Trigger a data source fetch by queueing the provider's sources."""
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

        queued = 0
        for s in sources:
            try:
                queue_result = queue_provider_data(
                    db,
                    redis_client,
                    provider_uuid,
                    version_uuid,
                    UUID(s["source_uuid"]),
                    s["source_path"],
                )
            except HTTPException as e:
                console.print(
                    f"[red]{s.get('source_name') or s['source_uuid']}: {e.detail}[/red]"
                )
                continue

            status_ = queue_result.get("status")
            label = s.get("source_name") or s["source_uuid"]
            if status_ == "success":
                queued += 1
                console.print(f"[green]queued[/green] {label} ({s['source_uuid']})")
            elif status_ == "busy":
                console.print(f"[yellow]busy[/yellow] {label}: {queue_result.get('message')}")
            elif status_ == "outdated":
                console.print(f"[yellow]outdated[/yellow] {label}: {queue_result.get('message')}")
            else:
                console.print(f"[red]{label}: {queue_result}[/red]")

    console.print(f"\nQueued {queued}/{len(sources)} source(s).")


if __name__ == "__main__":
    app()
