from typing import List, Optional
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table
from sqlalchemy import text

from database import SessionLocal
from dependencies import get_minio_client
from services.course_fetch.ratio import clear_ratio_cache
from services.eter import (
    FetchStats,
    cache_raw,
    compute_ratios,
    fetch_eter_records,
    load_cached_records,
    load_provider_uri_map,
    push_ratios_to_fuseki,
    ratios_for_provider,
)
from services.providers import resolve_provider_uuid

eter_app = typer.Typer(
    help="ETER indicator operations (student-to-staff ratios)", no_args_is_help=True
)

console = Console()


def _die(message: str, code: int = 1) -> None:
    console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=code)


def _resolve(db, value: str) -> UUID:
    try:
        return resolve_provider_uuid(db, value)
    except HTTPException as e:
        _die(str(e.detail))


def _isced_label(field_uri: Optional[str]) -> str:
    if not field_uri:
        return "[dim]institution-wide[/dim]"
    return field_uri.rsplit("/", 1)[-1]


@eter_app.command("fetch")
def eter_fetch(
    year: int = typer.Option(
        ..., "--year", "-y", help="ETER reference year (BAS.REFYEAR)",
    ),
    country: Optional[List[str]] = typer.Option(
        None, "--country", "-c",
        help="Restrict to ISO country code(s); repeat for multiple. Default: all.",
    ),
    from_cache: bool = typer.Option(
        False, "--from-cache",
        help="Re-derive from the newest cached payload for this year instead of "
             "calling the ETER API.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Compute ratios and report, but write nothing to Fuseki.",
    ),
    reindex: bool = typer.Option(
        False, "--reindex", help="Reindex every course afterwards so the new ratios reach Meilisearch.",
    ),
) -> None:
    """Fetch ETER indicators and push student-to-staff ratios to the stats graph."""
    minio_client = get_minio_client()
    stats = FetchStats()
    cache_key: Optional[str] = None

    if from_cache:
        with console.status(f"Loading cached ETER payload for {year}..."):
            records, cache_key = load_cached_records(minio_client, year=year)
        if not records:
            _die(f"No usable cached ETER payload for year {year}")
    else:
        with console.status(f"Fetching ETER data for {year}..."):
            records, raw = fetch_eter_records(year=year, countries=country)
        if not records:
            _die(f"ETER returned no records for year {year}")
        if not dry_run:
            cache_key = cache_raw(minio_client, raw, year=year)

    stats.records = len(records)

    with console.status("Computing ratios..."):
        rows = compute_ratios(records, year=year, stats=stats)
    stats.ratios = len(rows)

    if not rows:
        _die("No ratios could be computed — check the ETER field mapping")

    with SessionLocal() as db:
        provider_uri_map = load_provider_uri_map(db)

    if dry_run:
        seen = set()
        for row in rows:
            if row.isced_f_broad is None:
                stats.institution_scope += 1
            else:
                stats.field_scope += 1
            if row.eter_id in seen:
                continue
            seen.add(row.eter_id)
            if row.eter_id in provider_uri_map:
                stats.matched_ids += 1
            else:
                stats.unmatched_ids.append(row.eter_id)

        preview = Table(title=f"Sample ratios — year {year} (dry run, nothing written)")
        preview.add_column("ETER id")
        preview.add_column("Scope")
        preview.add_column("Students", justify="right")
        preview.add_column("Staff", justify="right")
        preview.add_column("Ratio", justify="right")
        # Sample both scopes — the field-level branch is the one worth eyeballing.
        sample = (
            [r for r in rows if r.isced_f_broad is None][:10]
            + [r for r in rows if r.isced_f_broad is not None][:10]
        )
        for row in sample:
            preview.add_row(
                row.eter_id,
                _isced_label(row.isced_f_broad),
                f"{row.students:,.0f}",
                f"{row.staff:,.1f}",
                f"{row.ratio:,.2f}",
            )
        console.print(preview)
    else:
        with console.status(f"Pushing {len(rows)} ratio(s) to Fuseki..."):
            stats = push_ratios_to_fuseki(rows, provider_uri_map, year=year, stats=stats)
        clear_ratio_cache()

    summary = Table(title=f"ETER fetch summary — year {year}")
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")
    summary.add_row("Records seen", str(stats.records))
    summary.add_row("Ratios computed", str(stats.ratios))
    summary.add_row("  institution-wide", str(stats.institution_scope))
    summary.add_row("  ISCED-F broad field", str(stats.field_scope))
    summary.add_row("Dropped: incoherent staff breakdown", str(stats.dropped_incoherent))
    summary.add_row("Dropped: field implausible vs institution", str(stats.dropped_implausible))
    summary.add_row("ETER ids matched to a provider", str(stats.matched_ids))
    summary.add_row("ETER ids unmatched (skipped)", str(len(stats.unmatched_ids)))
    if not dry_run:
        summary.add_row("Nodes pushed", str(stats.pushed))
        summary.add_row(
            "Push failures",
            f"[red]{stats.failed}[/red]" if stats.failed else "0",
        )
    if cache_key:
        summary.add_row("Cached payload", cache_key)
    console.print(summary)

    if not stats.matched_ids:
        console.print(
            "[yellow]No ETER id matched a provider — is the provider registry "
            "populated (`provider refresh`)?[/yellow]"
        )

    if dry_run:
        console.print("\n[dim]Dry run: nothing was written.[/dim]")
        return

    if reindex:
        from cli.courses import courses_reindex

        console.print("\n[cyan]Reindexing all courses...[/cyan]")
        courses_reindex(course=None, provider=None, all_=True)
    else:
        console.print(
            "\n[dim]Run `python cli.py course reindex --all` to push the new "
            "ratios into Meilisearch.[/dim]"
        )

    if stats.failed:
        raise typer.Exit(code=2)


@eter_app.command("show")
def eter_show(
    provider: str = typer.Argument(..., help="Provider UUID, ETER id, or DEQAR id"),
) -> None:
    """Show the stored ETER ratios for one provider (all years and scopes)."""
    with SessionLocal() as db:
        provider_uuid = _resolve(db, provider)
        row = db.execute(
            text("SELECT base_id, eter_id, provider_name FROM provider WHERE provider_uuid = :uuid"),
            {"uuid": str(provider_uuid)},
        ).fetchone()

    if not row or row[0] is None:
        _die(f"Provider {provider_uuid} has no base_id, so no provider IRI")

    provider_uri = f"https://data.deqar.eu/institution/{row[0]}"
    console.print(
        f"{row[2] or provider_uuid}\n"
        f"Provider URI: [cyan]{provider_uri}[/cyan] "
        f"[dim](ETER id {row[1] or '—'})[/dim]"
    )

    with console.status("Querying the stats graph..."):
        ratios = ratios_for_provider(provider_uri)

    if not ratios:
        console.print("[yellow]No ETER ratios stored for this provider.[/yellow]")
        raise typer.Exit(code=2)

    table = Table(title=f"Student-to-staff ratios — {len(ratios)} observation(s)")
    table.add_column("Year")
    table.add_column("Scope")
    table.add_column("Ratio", justify="right")
    table.add_column("Node URI")
    for r in ratios:
        try:
            value = f"{float(r['value']):,.2f}"
        except (TypeError, ValueError):
            value = r.get("value") or "-"
        table.add_row(
            (r.get("year") or "-")[:4],
            _isced_label(r.get("field")),
            value,
            r.get("uri") or "-",
        )
    console.print(table)
