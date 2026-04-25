# MangaLake

End-to-end ETL pipeline for manga metadata: `API -> MinIO -> Snowflake -> marts` with Apache Airflow orchestration.

![Architecture](image.png)

## Stack

- Python 3.11
- Apache Airflow
- MinIO (S3 compatible)
- Snowflake
- Docker / Docker Compose
- pytest + GitHub Actions

## Repository structure

```text
mangalake/
├── dags/
├── etl/
│   ├── clients/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   └── utils/
├── tests/
├── .github/workflows/
├── docker-compose.yml
├── env.example
└── README.md
```

## Local run (only with `.env`)

`.env` is used only for local development/testing and is not used in GitHub Actions.

1. Create local env file:

```bash
cp env.example .env
```

2. Fill required values in `.env` (at least Snowflake + local service credentials).

3. Start services:

```bash
docker-compose up -d --build
```

4. Open UIs:

- Airflow: `http://localhost:8081`
- MinIO: `http://localhost:9001`
- Superset: `http://localhost:88`

## Environment variables

### Local only (`.env`)

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `SUPERSET_DB`
- `SUPERSET_SECRET_KEY`
- `MINIO_ENDPOINT_URL`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET_NAME`
- `MANGA_API_BASE`
- `MANGA_API_FALLBACK`
- `REQUEST_TIMEOUT`
- `REQUEST_RETRIES`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

### GitHub Actions secrets

Set these in `Settings -> Secrets and variables -> Actions`:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

## Tests

Unit tests are in `tests/` and cover:

- API page parsing behavior (`etl/extract/manga_api.py`)
- Transform mapping logic (`etl/transform/manga_transform.py`)
- DataFrame normalization before Snowflake load (`etl/load/snowflake_load.py`)

Run locally:

```bash
python -m pytest -q
```

## CI/CD

### CI (`.github/workflows/ci.yml`)

Runs on push/PR:

- install dependencies
- run unit tests (`pytest -q`)

### CD (`.github/workflows/cd.yml`)

Runs on push to `main` and manually:

- build Docker image from `Dockerfile`
- push image to `ghcr.io/<owner>/<repo>`
- validate that Snowflake credentials are configured via GitHub Secrets

## Airflow pipelines

- `raw_from_api_to_s3`: extract raw manga data and store JSONL in MinIO
- `raw_from_s3_to_snowflake`: transform raw and upsert into `ODS_MANGA`
- `fct_count_day_manga`: build `DM_MANGA_DAILY_COUNTS`
- `fct_avg_day_manga`: build `DM_MANGA_AVG_YEAR`

## Notes

- `.env` is ignored by git and should never contain production secrets intended for CI/CD.
- CI/CD reads secrets from GitHub Actions only.
