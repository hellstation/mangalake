# MangaLake

End-to-end ETL pipeline for manga metadata processing and analytics.

## Overview

MangaLake is a data lakehouse implementation that demonstrates modern data engineering practices. It extracts manga metadata from APIs, stores raw data in object storage, transforms it, and loads it into a data warehouse for analytics and visualization.

The pipeline follows the "API → Lake → Warehouse → BI" architecture pattern, showcasing scalable ETL processes with proper error handling, retry logic, and data quality controls.

## Architecture

```
API → Airflow → MinIO → Snowflake → Superset
     ↓         ↓         ↓         ↓
  Extract  Transform  Store    Analyze
```

### Data Flow
1. **Extract**: Fetch manga data from APIs with fallback mechanisms
2. **Load Raw**: Store JSONL files in MinIO (S3-compatible storage)
3. **Transform**: Clean and normalize data using Pandas
4. **Load ODS**: Upsert data into Snowflake data warehouse
5. **Build Marts**: Create dimensional models for analytics
6. **Visualize**: Explore data through Superset dashboards

## Technology Stack

- **Orchestration**: Apache Airflow
- **Object Storage**: MinIO (S3-compatible)
- **Data Warehouse**: Snowflake
- **Business Intelligence**: Apache Superset
- **Runtime**: Python 3.9+
- **Containerization**: Docker & Docker Compose
- **Libraries**: requests, boto3, snowflake-connector, pandas

## Features

- Automated data extraction from multiple APIs with retry and fallback logic
- Scalable storage in MinIO with partitioning by load date
- Incremental loading with merge operations in Snowflake
- Data quality checks and error handling
- Docker-based deployment for easy setup
- Modular ETL code with proper separation of concerns
- Comprehensive logging and monitoring

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Snowflake account with warehouse access
- Internet connection for API access

### Installation

1. **Clone repository**
   ```bash
   git clone https://github.com/hellstation/mangalake.git
   cd mangalake
   ```

2. **Configure environment**
   ```bash
   cp env.example .env
   # Edit .env with your Snowflake credentials and other settings
   ```

3. **Start services**
   ```bash
   docker-compose up -d --build
   ```

4. **Access applications**
   - Airflow UI: http://localhost:8081 (admin/admin)
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
   - Superset: http://localhost:8088 (admin/admin)

5. **Run pipeline**
   - Open Airflow UI
   - Enable all DAGs
   - Trigger `raw_from_api_to_s3` DAG
   - Subsequent DAGs will run automatically

## Configuration

Create `.env` file based on `env.example`:

### Required Environment Variables

#### Snowflake
```env
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

#### MinIO
```env
MINIO_ENDPOINT_URL=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_NAME=manga-data
```

#### API (Optional)
```env
MANGA_API_BASE=https://api.mangadex.org
MANGA_API_FALLBACK=https://api.mangadex.org
REQUEST_TIMEOUT=30
REQUEST_RETRIES=3
```

Configuration details are in `etl/config.py`.

## Data Pipeline

### DAGs

#### `raw_from_api_to_s3`
Extracts manga data from APIs and stores as JSONL in MinIO.
- **Schedule**: Daily
- **Output**: `raw/manga/load_date=YYYY-MM-DD/manga_*.jsonl`

#### `raw_from_s3_to_snowflake`
Transforms raw data and loads into Snowflake ODS layer.
- **Dependencies**: `raw_from_api_to_s3`
- **Output**: `ODS_MANGA` table

#### `fct_count_day_manga`
Builds daily manga counts by status.
- **Output**: `DM_MANGA_DAILY_COUNTS` table

#### `fct_avg_day_manga`
Calculates average publication year by status.
- **Output**: `DM_MANGA_AVG_YEAR` table

### Data Models

#### ODS_MANGA (Operational Data Store)
```sql
MANGA_ID STRING PRIMARY KEY,
TITLE STRING,
STATUS STRING,
LAST_CHAPTER STRING,
YEAR INTEGER,
TAGS STRING,
UPDATED_AT TIMESTAMP,
LOAD_DATE DATE
```

#### DM_MANGA_DAILY_COUNTS (Data Mart)
```sql
LOAD_DATE DATE,
STATUS STRING,
COUNT_MANGA INTEGER
```

#### DM_MANGA_AVG_YEAR (Data Mart)
```sql
LOAD_DATE DATE,
STATUS STRING,
AVG_YEAR FLOAT
```

## Project Structure

```
mangalake/
├── dags/                     # Airflow DAG definitions
│   ├── raw_from_api_to_s3.py
│   ├── raw_from_s3_to_snowflake.py
│   ├── fct_count_day_manga.py
│   ├── fct_avg_day_manga.py
│   └── manga_pipeline_dag.py
├── etl/                      # ETL logic
│   ├── config.py            # Configuration
│   ├── clients/             # External service clients
│   │   ├── minio_client.py
│   │   └── snowflake_client.py
│   ├── extract/             # Data extraction
│   │   └── manga_api.py
│   ├── transform/           # Data transformation
│   │   └── manga_transform.py
│   ├── load/                # Data loading
│   │   └── snowflake_load.py
│   └── utils/               # Utilities
│       └── jsonl.py
├── postgres/                # PostgreSQL configs for Airflow
├── docker-compose.yml       # Service orchestration
├── Dockerfile              # Application container
├── requirements.txt        # Python dependencies
├── env.example            # Environment template
└── README.md
```

## Operations

### Monitoring

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow
docker-compose logs -f minio
docker-compose logs -f snowflake
```

### Data Reprocessing

```bash
# Backfill specific date
docker-compose exec airflow-scheduler airflow dags backfill raw_from_api_to_s3 \
  --start-date 2024-01-01 --end-date 2024-01-01
```

### Scaling

- Increase `page_size` in DAGs for faster extraction
- Adjust `batch_size` in `fetch_and_store_jsonl` for memory optimization
- Scale Airflow workers for higher throughput

## Troubleshooting

### Common Issues

**Pipeline fails at extraction**
- Check API endpoints in `.env`
- Verify internet connectivity
- Review Airflow logs for specific errors

**Snowflake connection errors**
- Validate credentials in `.env`
- Ensure warehouse is active
- Check network access to Snowflake

**MinIO storage issues**
- Verify MinIO service is running
- Check bucket permissions
- Validate endpoint URL

**Data quality problems**
- Check source API response format
- Review transformation logic in `manga_transform.py`
- Validate data types in Snowflake tables

### Logs

All components log to stdout/stderr. Use `docker-compose logs` to troubleshoot issues.

### Reset Environment

```bash
# Stop and remove all data
docker-compose down -v
docker-compose up -d --build
```

## Development

### Code Quality

- Type hints throughout codebase
- Comprehensive error handling
- Modular design with single responsibilities
- Proper logging with appropriate levels

### Testing

Run tests (when implemented):
```bash
python -m pytest
```


