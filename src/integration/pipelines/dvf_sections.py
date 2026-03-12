"""
DVF pipeline — Median prices per cadastral section.

Sources:
- DVF open data: https://files.data.gouv.fr/geo-dvf/latest/csv/
- Cadastral sections (Etalab): https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements/
"""

import tempfile
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
from rich.console import Console
from sqlalchemy import text

from integration.db import engine, ensure_postgis

console = Console()

DVF_BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv"
CADASTRE_BASE_URL = (
    "https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/departements"
)

TABLE_NAME = "dvf_sections"


def download_dvf(year: int, dep: str, dest: Path) -> Path:
    """Download DVF CSV for a given department and year."""
    url = f"{DVF_BASE_URL}/{year}/departements/{dep}.csv.gz"
    out = dest / f"dvf_{dep}_{year}.csv.gz"
    if out.exists():
        return out
    console.print(f"  Downloading DVF {dep} {year}...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    return out


def download_sections(dep: str, dest: Path) -> Path:
    """Download cadastral sections GeoJSON."""
    url = f"{CADASTRE_BASE_URL}/{dep}/cadastre-{dep}-sections.json.gz"
    out = dest / f"sections_{dep}.json.gz"
    if out.exists():
        return out
    console.print(f"  Downloading cadastral sections {dep}...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=8192):
                f.write(chunk)
    return out


def build_section_id(row: pd.Series) -> str:
    """Build section identifier: CODE_COMMUNE + SECTION."""
    commune = str(row["code_commune"])
    section = str(row["section_prefixe"]) + str(row["code_section"]) if pd.notna(row.get("section_prefixe")) else str(row["code_section"])
    return f"{commune}-{section.strip()}"


def aggregate_dvf(dvf_path: Path) -> pd.DataFrame:
    """Aggregate DVF mutations per cadastral section."""
    cols = [
        "id_mutation",
        "nature_mutation",
        "valeur_fonciere",
        "code_commune",
        "code_postal",
        "type_local",
        "surface_reelle_bati",
        "nombre_pieces_principales",
        "surface_terrain",
        "section_prefixe",
        "code_section",
    ]
    df = pd.read_csv(dvf_path, usecols=cols, low_memory=False)

    # Keep only sales with a price
    df = df[df["nature_mutation"] == "Vente"].dropna(subset=["valeur_fonciere"])

    df["section_id"] = df.apply(build_section_id, axis=1)

    # Price per m² for built properties
    bati = df[df["surface_reelle_bati"] > 0].copy()
    bati["prix_m2"] = bati["valeur_fonciere"] / bati["surface_reelle_bati"]

    agg = (
        bati.groupby("section_id")
        .agg(
            prix_m2_median=("prix_m2", "median"),
            prix_m2_mean=("prix_m2", "mean"),
            nb_ventes=("id_mutation", "nunique"),
            surface_mediane=("surface_reelle_bati", "median"),
        )
        .reset_index()
    )
    return agg


def load_sections_geom(sections_path: Path) -> gpd.GeoDataFrame:
    """Load cadastral section geometries."""
    gdf = gpd.read_file(sections_path)
    # Build compatible section identifier
    gdf["section_id"] = gdf["commune"] + "-" + gdf["prefixe"].fillna("") + gdf["code"]
    # Clean up whitespace
    gdf["section_id"] = gdf["section_id"].str.replace(" ", "")
    return gdf[["section_id", "geometry"]]


def run(year: int, departements: list[str]):
    """Main pipeline: download, aggregate, and load into DB."""
    ensure_postgis()

    with tempfile.TemporaryDirectory(prefix="geo-dvf-") as tmpdir:
        tmp = Path(tmpdir)
        all_frames = []

        for dep in departements:
            console.print(f"\n[bold]Department {dep}[/bold]")

            # 1. Download data
            dvf_path = download_dvf(year, dep, tmp)
            sections_path = download_sections(dep, tmp)

            # 2. Aggregate DVF by section
            console.print("  Aggregating DVF prices...")
            dvf_agg = aggregate_dvf(dvf_path)
            console.print(f"  -> {len(dvf_agg)} sections with sales")

            # 3. Load geometries
            console.print("  Loading geometries...")
            sections_geom = load_sections_geom(sections_path)

            # 4. Join
            merged = sections_geom.merge(dvf_agg, on="section_id", how="inner")
            merged["year"] = year
            merged["departement"] = dep
            console.print(f"  -> {len(merged)} sections with price and geometry")

            all_frames.append(merged)

        if not all_frames:
            console.print("[red]No data to load.[/red]")
            return

        final = gpd.GeoDataFrame(pd.concat(all_frames, ignore_index=True))
        final = final.set_crs(epsg=4326)

        # 5. Load into database
        console.print(f"\n[bold]Loading into database ({TABLE_NAME})...[/bold]")
        final.to_postgis(
            TABLE_NAME,
            engine,
            if_exists="replace",
            index=False,
            dtype={"geometry": "Geometry"},
        )

        # Create spatial index
        with engine.connect() as conn:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_geom ON {TABLE_NAME} USING GIST (geometry)"))
            conn.commit()

        console.print(f"[green]Done — {len(final)} sections loaded into {TABLE_NAME}[/green]")
