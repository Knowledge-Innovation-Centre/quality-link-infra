from typing import Optional
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from config import DEQAR_API_URL
from database import SessionLocal
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


providers_app = typer.Typer(help="Provider and data source operations", no_args_is_help=True)
oauth_app = typer.Typer(help="Manage out-of-band OAuth 2.0 client credentials", no_args_is_help=True)
providers_app.add_typer(oauth_app, name="oauth")

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


@providers_app.command("refresh")
def providers_refresh_from_deqar(
    limit: int = typer.Option(2000, "--limit", min=1, help="Page size"),
    offset: int = typer.Option(0, "--offset", min=0, help="Initial page offset"),
    api_url: str = typer.Option(DEQAR_API_URL, "--api-url", help="DEQAR providers endpoint"),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-upload all providers to Fuseki even if DEQAR metadata is unchanged",
    ),
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
            stats = upsert_providers(db, providers, force=force)

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
    table.add_row("Force", str(force))
    table.add_row("Fuseki success", str(push_stats.success))
    table.add_row("Fuseki failed", str(push_stats.failed))
    console.print(table)


def _mask_secret(secret: str) -> str:
    if not secret:
        return "-"
    if len(secret) <= 6:
        return "*" * len(secret)
    return f"{secret[:2]}{'*' * (len(secret) - 4)}{secret[-2:]}"


@oauth_app.command("set")
def oauth_set(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
    endpoint: str = typer.Option(..., "--endpoint", help="OAuth token endpoint URL"),
    client_id: str = typer.Option(..., "--client-id", help="OAuth client_id"),
    client_secret: Optional[str] = typer.Option(
        None,
        "--client-secret",
        help="OAuth client_secret (prompted if omitted, to keep it out of shell history)",
    ),
    scope: Optional[str] = typer.Option(None, "--scope", help="Default OAuth scope (optional)"),
) -> None:
    """
    Set or update out-of-band OAuth 2.0 client credentials for a provider's
    token endpoint. Used when a manifest source declares
    auth.type = 'oauth2.0' without inline client_id/client_secret.
    """
    if client_secret is None:
        client_secret = typer.prompt("Client secret", hide_input=True)
    if not client_secret:
        _die("Client secret cannot be empty.")

    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        db.execute(
            text("""
                INSERT INTO provider_oauth_cred
                    (provider_uuid, token_endpoint, client_id, client_secret, scope)
                VALUES (:provider_uuid, :token_endpoint, :client_id, :client_secret, :scope)
                ON CONFLICT (provider_uuid, token_endpoint) DO UPDATE
                SET client_id = EXCLUDED.client_id,
                    client_secret = EXCLUDED.client_secret,
                    scope = EXCLUDED.scope,
                    updated_at = NOW()
            """),
            {
                "provider_uuid": str(provider_uuid),
                "token_endpoint": endpoint,
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": scope,
            },
        )
        db.commit()
    console.print(f"[green]Stored OAuth credentials[/green] for {provider_uuid} @ {endpoint}")


@oauth_app.command("list")
def oauth_list(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """List OAuth credentials configured for a provider (secret is masked)."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        rows = db.execute(
            text("""
                SELECT token_endpoint, client_id, client_secret, scope, updated_at
                FROM provider_oauth_cred
                WHERE provider_uuid = :provider_uuid
                ORDER BY token_endpoint
            """),
            {"provider_uuid": str(provider_uuid)},
        ).fetchall()

    if not rows:
        console.print(f"[yellow]No OAuth credentials configured for {provider_uuid}[/yellow]")
        raise typer.Exit(code=2)

    table = Table(title=f"OAuth credentials — {provider_uuid}")
    table.add_column("Token endpoint")
    table.add_column("Client ID")
    table.add_column("Client secret")
    table.add_column("Scope")
    table.add_column("Updated")
    for row in rows:
        updated = row[4].isoformat()[:16].replace("T", " ") if row[4] else "-"
        table.add_row(
            row[0],
            row[1],
            _mask_secret(row[2]),
            row[3] or "-",
            updated,
        )
    console.print(table)


@oauth_app.command("delete")
def oauth_delete(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
    endpoint: str = typer.Option(..., "--endpoint", help="OAuth token endpoint URL"),
) -> None:
    """Delete a stored OAuth credential for a provider's token endpoint."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        result = db.execute(
            text("""
                DELETE FROM provider_oauth_cred
                WHERE provider_uuid = :provider_uuid AND token_endpoint = :token_endpoint
            """),
            {"provider_uuid": str(provider_uuid), "token_endpoint": endpoint},
        )
        db.commit()

    if result.rowcount:
        console.print(f"[green]Deleted[/green] OAuth credentials for {provider_uuid} @ {endpoint}")
    else:
        console.print(f"[yellow]No OAuth credentials found[/yellow] for {provider_uuid} @ {endpoint}")
        raise typer.Exit(code=2)

