# geo-score-integration

ETL pipelines to load geospatial datasets into the [geo-score-back](../geo-score-back) PostGIS database.

## Setup

```bash
# Install dependencies
uv sync

# Copy and edit config
cp .env.example .env
```

The PostgreSQL/PostGIS database must be running (via geo-score-back's `docker-compose`):

```bash
cd ../geo-score-back/docker && docker compose up -d
```

## Usage

```bash
# Check DB connection
uv run geo-integrate check-db

# Load DVF prices per cadastral section (Paris, 2023)
uv run geo-integrate dvf --year 2023 --dep 75

# Multiple departments
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94
```

## DVF — Land value prices per cadastral section

Downloads DVF open data + cadastral section geometries from data.gouv.fr, aggregates median price/m² per section, and loads into PostGIS.

**Schema:** `dvf_sections` — one table per year (`y2023`, `y2022`, etc.)

**Columns:** `section_id`, `geometry`, `geom` (native PostGIS with GIST index), `prix_m2_median`, `prix_m2_mean`, `nb_ventes`, `surface_mediane`, `departement`

```bash
# Single department (Paris, 2023)
uv run geo-integrate dvf --year 2023 --dep 75

# Île-de-France
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94 --dep 77 --dep 78 --dep 91 --dep 95

# Different year → separate table (dvf_sections.y2022)
uv run geo-integrate dvf --year 2022 --dep 75

# Add departments to an existing year (idempotent — re-running same dep replaces its data)
uv run geo-integrate dvf --year 2023 --dep 13
uv run geo-integrate dvf --year 2023 --dep 69
```

## Pipelines

| Pipeline | Schema | Description |
|----------|--------|-------------|
| `dvf` | `dvf_sections` | Median price/m² per cadastral section (DVF + Etalab cadastre) |

## Architecture

```
src/integration/
├── config.py              # Configuration (DATABASE_URL via .env)
├── db.py                  # SQLAlchemy engine + session
├── cli.py                 # Typer CLI (entry point)
└── pipelines/
    └── dvf_sections.py    # DVF → cadastral sections ETL
```

## Adding a new pipeline

1. Create `src/integration/pipelines/my_pipeline.py` with a `run()` function
2. Add a command in `cli.py`
3. Document the created table in this README
