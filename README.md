# Steam Analytics Pipeline

An end-to-end, open-source data engineering pipeline that pulls live data from the Steam Web API and Steam Store API, processes it through a Medallion architecture (Bronze → Silver → Gold), models it into a dimensional star schema, and serves it to Power BI for analysis — orchestrated entirely with Apache Airflow running locally via Docker.

## Project overview

This project was built to practice real data engineering patterns — orchestration, data quality validation, incremental loads, and dimensional modeling — using entirely free and open-source tooling, with no cloud subscription required to run it end to end.

The pipeline tracks daily player counts and weekly pricing/store metadata for a set of tracked games, and surfaces the results as a queryable star schema ready for BI consumption.

## Architecture

```
Steam Web API + Store API
        ↓
Apache Airflow (Docker) — daily fact fetch, weekly dimension refresh (branching DAG)
        ↓
Bronze layer — raw JSON landed by ingestion date
        ↓
Silver layer — cleaned, validated, deduplicated (parquet)
        ↓
Gold layer — daily snapshot + trend aggregates (parquet)
        ↓
Star schema (SQLite) — fact_game_performance + dim_game, dim_date, dim_publisher
        ↓
Power BI — dashboards and visualizations
```

## Tech stack

- **Orchestration:** Apache Airflow (Docker Compose, local)
- **Ingestion:** Python, Steam Web API, Steam Store API
- **Processing:** pandas, PyArrow (parquet)
- **Storage:** Local filesystem (Bronze/Silver/Gold partitions), SQLite (star schema)
- **Modeling:** Star schema with a Type 2 slowly-changing dimension (`dim_publisher`)
- **Visualization:** Power BI Desktop

## Pipeline stages

### 1. Ingestion (Bronze)
- Daily: pulls current player counts for tracked games from the Steam Web API.
- Weekly (branch): pulls pricing, discount, genre, and publisher data from the Steam Store API.
- Raw JSON responses are landed into date-partitioned Bronze folders (`bronze/dt=YYYY-MM-DD/`).

### 2. Silver — data quality and validation
- Drops records with missing keys or unparseable types.
- Coerces and validates timestamps, rejecting anything malformed.
- Deduplicates on `appid`, keeping the latest pull per run.
- Flags a real Steam API data gap: paid games that report a `$0` price ("price_data_missing").
- Every run writes a JSON quality log (`silver/_quality_logs/dt=YYYY-MM-DD.json`) recording exactly how many records were dropped and why — this is the pipeline's own audit trail, not a one-off manual count.

### 3. Gold — aggregation
- Builds a daily snapshot joining player counts with pricing (`revenue_proxy` = price × concurrent players).
- Builds a rolling trend table: 7-day average player count, day-over-day % change, 7-day average discount.

### 4. Star schema
- `dim_date` — standard calendar dimension.
- `dim_publisher` — Type 2 slowly-changing dimension (tracks publisher history over time).
- `dim_game` — one row per tracked game, joined to its current publisher.
- `fact_game_performance` — daily grain, one row per game per day, linked to both dimensions via surrogate keys.

### 5. Orchestration (Airflow DAG)
- Daily player-count fetch runs unconditionally.
- A `BranchPythonOperator` checks whether the weekly store-details refresh should run, joining back via `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` so a skipped branch never blocks downstream tasks.
- Tasks are chained with explicit dependencies: ingestion → Bronze landing → Silver transform → Gold build → star schema build.
- Each task has retry logic (3 retries, 5-minute delay) to handle transient API failures.

## Running it locally

**Prerequisites:** Docker Desktop, Python 3.x

```bash
git clone https://github.com/<your-username>/steam-analytics-pipeline.git
cd steam-analytics-pipeline
docker compose up airflow-init
docker compose up -d
```

Airflow UI: `http://localhost:8080` (default credentials: `airflow` / `airflow`)

Enable and trigger `steam_pipeline_dag` from the UI to run the full pipeline.

## Exporting to Power BI

```bash
python scripts/export_gold_to_csv.py
```

This reads the star schema tables from SQLite and writes them to `warehouse/gold/` as CSVs, ready to load into Power BI Desktop via **Get Data → Text/CSV**. Relationships are built on `game_key`, `date_key`, and `publisher_key`.

## Project structure

```
steam-analytics-pipeline/
├── dags/
│   └── steam_pipeline_dag.py
├── scripts/
│   ├── fetch_player_counts.py
│   ├── fetch_store_details.py
│   ├── transform_silver.py
│   ├── build_gold.py
│   ├── build_star_schema.py
│   └── export_gold_to_csv.py
├── bronze/          (gitignored — generated data)
├── silver/          (gitignored — generated data)
├── gold/            (gitignored — generated data)
├── warehouse/       (gitignored — generated data)
├── docker-compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

## Notes and honest limitations

- This runs locally rather than on cloud infrastructure — the architecture (Medallion layers, star schema, orchestration patterns) is designed to be portable to Azure/ADLS Gen2 or AWS S3 with minimal changes to the storage layer.
- The Steam Web API only exposes *current* player counts, not historical data — so trend data accumulates in real time as the pipeline runs on schedule, rather than being backfilled.
- Power BI refresh is currently manual (re-run the export script, then Refresh in Power BI Desktop) rather than fully automated.

## What this project demonstrates

- Dimensional data modeling (fact/dimension tables, surrogate keys, a Type 2 SCD)
- Data quality validation as a first-class pipeline step, not an afterthought
- Conditional DAG logic (branching, trigger rules) beyond a simple linear chain
- End-to-end ownership from raw API ingestion through to BI-ready output
