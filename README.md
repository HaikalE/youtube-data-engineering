# YouTube Trending Analytics Data Pipeline

End-to-end data engineering project that collects YouTube trending-video data, transforms it with Python, persists raw/processed outputs, and exposes the results through a Plotly Dash dashboard.

The project was built to practice the parts of a data platform that sit between an API call and a dashboard: repeatable extraction, transformation, storage, orchestration, validation, and operational troubleshooting.

## Architecture

```text
YouTube Data API v3
        |
        v
Python extraction
        |
        +--------------------> Amazon S3
        |                     raw / processed artifacts
        v
Transformation and validation
        |
        v
PostgreSQL / local development storage
        |
        +--------------------> analytics scripts
        |
        v
Plotly Dash dashboard

Apache Airflow orchestrates the scheduled workflow.
```

## What the project demonstrates

- API-based data extraction with Python
- modular extract / transform / load stages
- Apache Airflow DAG orchestration
- raw and processed object storage in Amazon S3
- structured persistence for analytical queries
- derived engagement metrics and category/channel analysis
- operational checks for database and S3 data
- interactive analytics with Plotly Dash

## Repository structure

```text
.
├── config/                  pipeline configuration
├── dags/
│   └── youtube_trending_dag.py
├── dashboard/
│   ├── app.py
│   └── assets/
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── analyze.py
├── utils/                   database and storage helpers
├── check_data.py            local data validation helper
├── check_db.py              database verification helper
├── check_s3.py              S3 verification helper
├── start.sh                 local workflow launcher
└── requirements.txt
```

## Dashboard

The dashboard presents views for category distribution, content performance, and top-performing videos/channels.

![Category analysis dashboard](https://github.com/user-attachments/assets/b47d41eb-46ae-4e42-82a6-8177bd4b6d12)

![Top performers dashboard](https://github.com/user-attachments/assets/00887941-81bc-4239-b995-33c78afa82b8)

## Local setup

Requirements depend on which parts of the pipeline you want to run. The full workflow expects Python, YouTube Data API credentials, database configuration, and AWS credentials for S3-backed storage.

```bash
git clone https://github.com/HaikalE/youtube-data-engineering.git
cd youtube-data-engineering
pip install -r requirements.txt
```

Configure the environment and `config/config.yaml`, then individual stages can be run with:

```bash
python -m scripts.extract config/config.yaml
python -m scripts.transform config/config.yaml <raw-data-file>
python -m scripts.load config/config.yaml <raw-data-file> <processed-data-file>
python -m scripts.analyze config/config.yaml <processed-data-file>
```

The dashboard can be started with:

```bash
python -m dashboard.app
```

The repository also includes `start.sh` for launching project components in the local development environment.

## Airflow

`dags/youtube_trending_dag.py` defines the orchestrated workflow. Airflow is used here to make dependencies between pipeline stages explicit and to move the project beyond a manually executed sequence of scripts.

## Engineering notes

This is a portfolio data-engineering system rather than a claim of production infrastructure. The codebase includes utilities and troubleshooting scripts created while validating S3, database, and dashboard behavior. Generated caches, compiled Python files, local databases, credentials, and runtime logs are excluded from source control.

Areas I would harden for a production deployment include automated test coverage, secrets management, schema/version contracts, observability, retry/idempotency strategy, containerized deployment, and infrastructure-as-code.

## Author

Muhammad Haikal Rahman  
[GitHub](https://github.com/HaikalE) · [Portfolio](https://haikale.github.io) · [LinkedIn](https://www.linkedin.com/in/muhammad-haikal-rahman)
