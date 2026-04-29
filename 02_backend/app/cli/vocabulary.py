from typing import List, Optional, Annotated
from uuid import UUID

import typer
from fastapi import HTTPException
from rich.console import Console
from rich.table import Table

from config import DEFAULT_VOCABULARIES, DEQAR_API_URL
from database import SessionLocal

from services.vocabulary import refresh_vocabulary


vocabularies_app = typer.Typer(help="EU controlled vocabulary operations", no_args_is_help=True)

console = Console()


def _die(message: str, code: int = 1) -> None:
    console.print(f"[red]{message}[/red]")
    raise typer.Exit(code=code)


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

