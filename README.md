# geo-score-integration

Pipelines ETL pour charger des jeux de données géospatiaux dans la base PostGIS de [geo-score-back](../geo-score-back).

## Setup

```bash
# Installer les dépendances avec uv
uv sync

# Copier et adapter la config
cp .env.example .env
```

La base PostgreSQL/PostGIS doit tourner (via le `docker-compose` de geo-score-back) :

```bash
cd ../geo-score-back/docker && docker compose up -d
```

## Utilisation

```bash
# Vérifier la connexion DB
uv run geo-integrate check-db

# Charger les prix DVF par section cadastrale (Paris, 2023)
uv run geo-integrate dvf --year 2023 --dep 75

# Plusieurs départements
uv run geo-integrate dvf --year 2023 --dep 75 --dep 92 --dep 93 --dep 94
```

## Pipelines disponibles

| Pipeline | Table | Description |
|----------|-------|-------------|
| `dvf` | `dvf_sections` | Prix médians au m² par section cadastrale (source: DVF + cadastre Etalab) |

## Architecture

```
src/integration/
├── config.py              # Configuration (DATABASE_URL via .env)
├── db.py                  # Engine SQLAlchemy + session
├── cli.py                 # CLI Typer (point d'entrée)
└── pipelines/
    └── dvf_sections.py    # ETL DVF → sections cadastrales
```

## Ajouter un nouveau pipeline

1. Créer `src/integration/pipelines/mon_pipeline.py` avec une fonction `run()`
2. Ajouter une commande dans `cli.py`
3. Documenter la table créée dans ce README
