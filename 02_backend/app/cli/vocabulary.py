from typing import Dict, List, Optional

import typer
from rich.console import Console
from rich.table import Table

from config import DEFAULT_VOCABULARIES

from services.vocabulary import refresh_vocabulary, _normalize_spec


vocabularies_app = typer.Typer(help="EU controlled vocabulary operations", no_args_is_help=True)

console = Console()


def _configured_properties() -> Dict[str, List[str]]:
    """Map scheme_uri -> extra properties list, derived from DEFAULT_VOCABULARIES."""
    out: Dict[str, List[str]] = {}
    for entry in DEFAULT_VOCABULARIES:
        scheme, props = _normalize_spec(entry)
        out[scheme] = props
    return out


@vocabularies_app.command("fetch")
def vocabularies_fetch(
    schemes: Optional[List[str]] = typer.Argument(
        default=None,
        help="Scheme URIs to fetch; repeat for multiple. Extra properties to fetch "
             "for a given scheme are taken from DEFAULT_VOCABULARIES in config. "
             "Default (no args): fetch every scheme in DEFAULT_VOCABULARIES.",
    ),
) -> None:
    """
    Fetch controlled vocabularies from EU Publications and push to Fuseki.
    """
    configured = _configured_properties()

    if schemes:
        specs = [
            {"scheme": uri, "properties": configured.get(uri, [])}
            for uri in schemes
        ]
    else:
        specs = list(DEFAULT_VOCABULARIES)

    table = Table(title=f"Vocabulary fetch — {len(specs)} scheme(s)")
    table.add_column("Scheme URI")
    table.add_column("Concepts", justify="right")
    table.add_column("Extras", justify="right")
    table.add_column("Bytes", justify="right")
    table.add_column("Result")

    any_failed = False
    for spec in specs:
        scheme, _ = _normalize_spec(spec)
        with console.status(f"Fetching {scheme}..."):
            stats = refresh_vocabulary(spec)
        result = "[green]ok[/green]" if stats.success else f"[red]{stats.error or 'failed'}[/red]"
        table.add_row(
            scheme,
            str(stats.concepts),
            str(stats.extra_triples),
            str(stats.bytes_uploaded),
            result,
        )
        if not stats.success:
            any_failed = True

    console.print(table)
    if any_failed:
        raise typer.Exit(code=2)
