#!/usr/bin/env python3

import logging

import typer

from cli.courses import courses_app
from cli.providers import providers_app
from cli.vocabulary import vocabularies_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = typer.Typer(help="QualityLink pipeline admin CLI", no_args_is_help=True)

app.add_typer(providers_app, name="provider")
app.add_typer(vocabularies_app, name="vocabulary")
app.add_typer(courses_app, name="course")

if __name__ == "__main__":
    app()

