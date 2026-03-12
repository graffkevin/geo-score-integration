import typer
from rich.console import Console

app = typer.Typer(name="geo-integrate", help="Load geospatial datasets into geo-score DB.")
console = Console()


@app.command()
def dvf(
    year: int = typer.Option(2023, help="DVF year to load"),
    departements: list[str] = typer.Option(
        ["75"], "--dep", help="Department codes (e.g. 75 92 93)"
    ),
):
    """Load median DVF prices per cadastral section."""
    from integration.pipelines.dvf_sections import run

    run(year=year, departements=departements)


@app.command()
def check_db():
    """Check database connection."""
    from sqlalchemy import text

    from integration.db import engine

    with engine.connect() as conn:
        result = conn.execute(text("SELECT PostGIS_Version()")).scalar()
        console.print(f"[green]Connected — PostGIS {result}[/green]")


if __name__ == "__main__":
    app()
