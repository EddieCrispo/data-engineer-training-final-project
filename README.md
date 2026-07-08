# data-engineer-toolkit

## retail medallion pipeline (final project)

Incremental medallion ETL (bronze -> silver -> gold) for retail CSV data, orchestrated by
Airflow, landing in a Postgres star-schema datamart plus a set of analysis views. Requires
both the `airflow` and `postgres` profiles.

### Run it

```bash
# build images
docker compose --profile airflow --profile postgres build

# bring up the DBs first, then run Airflow's init (db migrate + create admin user)
docker compose --profile airflow --profile postgres up -d airflow-postgres postgres statsd-exporter
docker compose --profile airflow run --rm airflow-init

# now start everything
docker compose --profile airflow --profile postgres up -d
```

Airflow UI: http://localhost:8080 - login `airflow` / `airflow` (from `airflow/.env`).

Warehouse Postgres is reachable on the host at `localhost:5434`, db `warehouse`,
user `root`, password `dibimbing` (from `db/.env`).

### Generate data and run the pipeline

```bash
docker compose exec airflow-webserver python /opt/airflow/dataset_generator/retail_csv_generator.py \
  --unique-id "<your-id>" --output-dir /opt/airflow/data/raw --once

docker compose exec airflow-webserver airflow dags unpause retail_medallion_pipeline
docker compose exec airflow-webserver airflow dags trigger retail_medallion_pipeline
```

Files land at `./airflow/data/raw/<your-id>/{table}/{table}_{timestamp}.csv` on the host.

DAG `retail_medallion_pipeline` task order: `init_schemas -> bronze_ingest ->
silver_transform -> gold_build_dims -> gold_build_fact -> refresh_analysis_views`,
scheduled every 15 minutes.

### Analysis report

```bash
docker compose exec airflow-webserver python /opt/airflow/dags/scripts/run_analysis_report.py
```

Outputs chart PNGs + `summary.md` to `./airflow/data/analysis_report/` on the host.

## airflow

`docker compose --profile airflow up -d`

## grafana

`docker compose --profile grafana up -d`

## postgres

`docker compose --profile postgres up -d`
or `docker compose --profile db up -d` (mysql + postgres)

## mysql

`docker compose --profile mysql up -d`
or `docker compose --profile db up -d` (mysql + postgres)

## hive

`docker compose --profile hive up -d`

## kafka

`docker compose --profile kafka up -d`

## spark

`docker compose --profile spark up -d`

