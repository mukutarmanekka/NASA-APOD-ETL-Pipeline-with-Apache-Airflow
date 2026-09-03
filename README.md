# 🚀 NASA APOD ETL Pipeline

An automated, containerized ETL pipeline that pulls NASA's **Astronomy Picture of the Day (APOD)** every day, transforms it, and loads it into a PostgreSQL database — orchestrated end-to-end with **Apache Airflow** on the **Astro CLI**.

![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

---

## 📌 Overview

This project implements a small, production-style ELT/ETL workflow:

1. **Extract** — call the [NASA APOD API](https://api.nasa.gov/) for the day's astronomy picture and metadata.
2. **Transform** — parse the JSON response and shape it into a clean record.
3. **Load** — insert the record into a `apod_data` table in PostgreSQL.

The whole thing runs as a single Airflow DAG (`project1_nasa`) on a daily schedule, packaged with Docker so the entire stack — Airflow, its metadata database, and the pipeline's destination database — can be spun up with one command.

## 🏗️ Architecture

![ETL Pipeline Architecture](Outputs/ETL%20Pipeline%20Architecture.png)

**Flow:** `NASA APOD API → Airflow (create_table → extract_apod → transform_apod_data → load_data_to_postgres) → PostgreSQL`

## ⚙️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow (TaskFlow API + `HttpOperator`) |
| Dev environment | Astro CLI (Astronomer) |
| Language | Python |
| Database | PostgreSQL |
| Containerization | Docker & Docker Compose |
| Data source | NASA APOD REST API |

## 📂 Project Structure

```
Project1/
├── dags/
│   └── etl.py                    # DAG: project1_nasa — the ETL pipeline
├── include/                      # Extra project files (empty)
├── plugins/                      # Custom Airflow plugins (empty)
├── tests/
│   └── dags/
│       └── test_dag_example.py   # DagBag validation tests (import errors, tags, retries)
├── Outputs/                      # Architecture diagram & Airflow UI screenshots
│   ├── ETL Pipeline Architecture.png
│   ├── DAG Run.png
│   └── Task Instances.png
├── Dockerfile                    # Astro Runtime image
├── docker-compose.yaml           # Standalone Postgres container (pipeline destination DB)
├── airflow_settings.yaml         # Local Airflow connections / variables / pools
├── requirements.txt              # apache-airflow-providers-http, -postgres
├── packages.txt                  # OS-level packages (none required)
└── .gitignore
```

## 🔄 How the Pipeline Works

The DAG lives in [`dags/etl.py`](dags/etl.py) and defines four tasks wired into a straight-line dependency chain:

```
create_table >> extract_apod >> transform_apod_data >> load_data_to_postgres
```

| # | Task | Type | What it does |
|---|---|---|---|
| 1 | `create_table` | `@task` (PostgresHook) | Runs a `CREATE TABLE IF NOT EXISTS` for `apod_data`, so the DAG is idempotent on first run. |
| 2 | `extract_apod` | `HttpOperator` | Calls `GET planetary/apod` on the `nasa_api` connection, passing the API key from the connection's `extra` field, and returns the parsed JSON. |
| 3 | `transform_apod_data` | `@task` | Picks `title`, `explanation`, `url`, `date`, and `media_type` out of the raw response into a clean dict. |
| 4 | `load_data_to_postgres` | `@task` (PostgresHook) | Inserts the transformed record into `apod_data` via a parameterized `INSERT`. |

Because the tasks use Airflow's **TaskFlow API** (`@task`), the return value of one task is automatically passed to the next as its argument (via XCom) — `extract_apod.output` feeds `transform_apod_data`, and its output feeds `load_data_to_postgres`, with no manual XCom plumbing required.

**Schedule:** `@daily` — one run per day, `catchup=False` so no backfilling of historical dates on first deploy.

## 🗄️ Database Schema

`apod_data` (created automatically by the `create_table` task):

| Column | Type | Notes |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | Auto-incrementing row id |
| `title` | `VARCHAR(255)` | Title of the day's picture |
| `explanation` | `TEXT` | NASA's write-up for the picture |
| `url` | `TEXT` | Image (or video) URL |
| `date` | `DATE` | APOD date |
| `media_type` | `VARCHAR(50)` | `image` or `video` |

## 🚀 Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) (Desktop or Engine)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli/)
- A free [NASA API key](https://api.nasa.gov/)

### Setup

1. **Clone the repo**
   ```bash
   git clone <your-repo-url>
   cd Project1
   ```

2. **Start the destination database**
   ```bash
   docker-compose up -d
   ```
   This brings up a standalone `postgres_db` container (`localhost:5432`, user/password `postgres`) that the pipeline loads data into — separate from Airflow's own internal metadata database.

3. **Start Airflow**
   ```bash
   astro dev start
   ```
   This spins up the Airflow webserver, scheduler, DAG processor, triggerer, and its metadata Postgres, and opens the UI at [http://localhost:8080](http://localhost:8080).

4. **Add the two Airflow connections** (UI → Admin → Connections, or via `airflow_settings.yaml`):

   | Conn Id | Conn Type | Host | Extra / Login |
   |---|---|---|---|
   | `nasa_api` | HTTP | `https://api.nasa.gov` | Extra: `{"api_key": "YOUR_NASA_API_KEY"}` |
   | `postgres_default` | Postgres | `postgres_db` (or `host.docker.internal`) | Login `postgres`, Password `postgres`, Schema `postgres`, Port `5432` |

   > If Airflow can't reach `postgres_db` by container name, make sure the Astro project's Docker network and the `airflow_network` created by `docker-compose.yaml` can see each other (`docker network connect`), or point the connection at `host.docker.internal` instead.

5. **Trigger the DAG**
   In the Airflow UI, unpause and trigger `project1_nasa` — or just wait for the next scheduled run.

### Stopping

```bash
astro dev stop
docker-compose down
```

## 🖼️ Pipeline in Action

**A successful DAG run**, all four tasks green:

![DAG Run](Outputs/DAG%20Run.png)

**Task history across multiple scheduled runs:**

![Task Instances](Outputs/Task%20Instances.png)

## ✅ Testing

The project ships with a `pytest` suite (`tests/dags/test_dag_example.py`) that:
- Validates every DAG in the project imports without errors
- Checks every DAG carries tags
- Checks every DAG's `default_args` sets `retries >= 2`

Run it with:
```bash
astro dev pytest
```

> Note: `project1_nasa` currently defines no `tags` and no `default_args.retries`, so the tag/retry checks above will fail until those are added to the DAG — see **Possible Improvements** below.

## 📈 Possible Improvements

- Add `tags=[...]` and `default_args={"retries": 2, ...}` to `project1_nasa` to satisfy the bundled tests and make failures auto-retry.
- Add a `UNIQUE` constraint on `apod_data.date` (or an `ON CONFLICT DO NOTHING`/`UPSERT`) so manually re-triggering a run doesn't insert duplicate rows for the same date.
- Add a data-quality check task (e.g. non-null `url`) before the load step.
- Send a Slack/email alert on task failure via Airflow's `on_failure_callback`.
- Parameterize the destination table/schema name via an Airflow `Variable` instead of hardcoding it.

---

*Built with [Apache Airflow](https://airflow.apache.org/) and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/).*
