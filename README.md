# nyc-taxi-pipeline

An end-to-end data pipeline on NYC TLC Yellow Taxi trip data (in progress), built to learn and demonstrate core data engineering tools.

## Data

NYC TLC Yellow Taxi trips, monthly Parquet files, free and public, the files are not stored in the repo.

## Planned stack

* Docker / Compose: runs the services (Postgres, Airflow), so the setup works the same on any machine.
* Postgres: the database where the data is stored and queried with SQL.
* PySpark: processes large amounts of data, used to clean and aggregate the data.
* Airflow: schedules and runs the pipeline steps automatically.

## Status

- Week 1 (done): Postgres running via Docker Compose; raw August 2026 file loaded into a staging table (3,336,716 rows)
- Week 2 (done): clean table designed and loaded (3,336,715 rows); six analytical queries (q01 to q06); loader runs in a Docker container
- Week 3 (in progress): PySpark in a container. The cleaning job matches the Postgres clean table, the aggregation job writes output partitioned by pickup date, and a benchmark compares Spark with pandas.
- Airflow (in progress): Airflow 3.3.0 runs in Docker Compose (`airflow/`). `taxi_download` downloads and validates one monthly Parquet file, with the month as a parameter.

## Repo structure

```
docker/              docker-compose.yml (Postgres, loader, Spark), Dockerfile.loader, Dockerfile.spark
airflow/             Airflow's own docker-compose.yaml (official file, example DAGs off)
airflow/dags/        DAGs: hello_dag, taxi_download
scripts/             load_data.py (Parquet -> staging table)
spark_jobs/          PySpark jobs: 00_hello, 01_clean, 02_aggregate, 03_benchmark, 04_memory_test
sql/                 create_staging, create_clean, alter_clean, load_clean
sql/checks/          one-off queries I used to investigate the data and decide the cleaning rules
sql/queries/         analytical queries q01 to q06
data/                local Parquet files (not committed)
data/downloads/      files downloaded by Airflow (not committed)
output/              Spark output (not committed)
requirements.txt     Python dependencies for the loader
requirements-spark.txt  Python dependencies for the Spark image
```

## Data model

The data lands in two tables, so the raw file is never altered.

- `staging_yellow_trips`: the file exactly as loaded. 21 columns, no constraints.
- `clean_yellow_trips`: typed and constrained, with a surrogate `trip_id` and two flags: `has_passenger_count` and `is_reversal`.

Money columns are `NUMERIC(10,2)` in the clean table, not `DOUBLE PRECISION` as in staging. Floating point cannot represent most decimal amounts exactly, so sums of fares would drift. Distance stays `DOUBLE PRECISION` because it is a measurement, not money.

The clean load is idempotent. `load_clean.sql` truncates the table, restarts the identity and inserts in one transaction, so a rerun gives identical rows and identical `trip_id` values.

## Cleaning decisions

I checked the data before loading. All 3,336,716 rows fit the `NUMERIC(10,2)` range (largest value 5,530.74), and no column that is `NOT NULL` in the clean table contains a NULL. Result: 3,336,715 rows in the clean table.

| Finding | Rows | Decision | Reason |
|---|---|---|---|
| No passenger count | 921,181 | Keep, flag `has_passenger_count` | A different population: average distance 8.93 vs 3.37 miles, average fare $25.14 vs $19.65. Dropping them would hide that; the flag lets each query include or exclude them. |
| Negative amounts (reversals) | 14,510 | Keep, flag `is_reversal` | Negatives appear in almost every money column, and 72.8% of negative fares have payment type 4 or 3. The file does not show whether each reversal's original trip is present, so the flag lets each query choose. Rule: `fare_amount < 0 OR total_amount < 0`. |
| Pickup in 2009 | 1 | Drop | A plausible trip with an impossible date. The true date cannot be recovered, so it cannot be placed in any hourly or zone aggregate. |
| Pickup in July 2026 | 14 | Keep | Valid timestamps on plausible trips, reported in the next month's file. |
| Pickup after dropoff | 3 | Keep | The true order cannot be recovered, but pickup time and fare are still correct for trips per hour and revenue per zone. |
| Zero passengers | 7,478 | Keep | Ordinary trips, slightly smaller than average (2.51 vs 3.37 miles, $17.80 vs $19.66). Already identifiable by `passenger_count = 0`, so no extra flag. |

A flag names a rule once, so queries don't each rewrite it. `is_reversal` is the clear case: it combines two columns and is easy to get wrong. `has_passenger_count` is the weaker case: `passenger_count IS NOT NULL` gives the same result, but the flag marks a whole block of rows with different behaviour (see the table above). Conditions that one existing column already shows, such as zero passengers, get no flag.

## Analytical queries

All queries run on `clean_yellow_trips` and exclude reversals (`WHERE NOT is_reversal`): a reversal is a refund or voided charge, not a trip. Revenue is therefore gross, about 0.43% above net if the original charges are in the file. The result tables below are for the August 2026 file.

| Query | Question | Finding |
|---|---|---|
| q01 | Trips per hour | Busiest hour is 18 (235,723 trips), quietest is 4 (31,023). |
| q02 | Top zones by trips, with revenue per trip and average miles | Zones 132 (JFK) and 138 (LaGuardia) earn $77.50 and $68.50 per trip against $21.78 to $29.94 elsewhere. Their average miles are 14.40 and 9.48 against 2.24 to 4.27, so the premium comes from trip length. |
| q03 | Top zones per payment type (window function) | Zones 132 and 138 rank first or second for payment types 1 to 4. Payment type 0 is led by zone 79. |
| q04 | Running total of daily revenue (window function) | Ends at $100,779,468.23, which matches the non-reversal revenue total. |
| q05 | Day-over-day revenue change (`LAG`) | Jul 31 is a partial day, so the Aug 1 change is meaningless. Aug 20 is a spike ($4,327,427.23, +$1,249,617.47). Mondays are the lowest days. |
| q06 | Aug 19 to 21: trips, revenue, revenue per trip | Trips rose 30% on Aug 20 (104,685 to 136,144) and fell back on Aug 21 (100,481). Revenue per trip moved from $29.40 to $31.79 and stayed at $31.29, so the spike is mainly more trips. |

## Spark jobs

The same cleaning rules as the Postgres clean table, applied in PySpark. Spark runs in its own container, so no Java is needed on the host.

| Job | What it does | Check |
|---|---|---|
| `00_hello.py` | Reads the August file and prints schema and row count | 3,336,716 rows, same as staging |
| `01_clean.py` | Filters pickups before 2020-01-01, adds `has_passenger_count` and `is_reversal`, writes Parquet | 3,336,715 rows, 14,510 reversals, 921,181 without passenger count, revenue $100,779,468.23 |
| `02_aggregate.py` | Trips per hour and revenue per zone (reversals excluded, money as `DECIMAL`); writes output partitioned by `pickup_date` | Same numbers as q01 and q02; reading Aug 19 to 21 touches only 3 of 32 partition folders (`PartitionFilters` in `explain()`) |
| `03_benchmark.py` | Times Spark against pandas | See below |
| `04_memory_test.py` | Runs one tool on all 24 columns, for the memory-capped comparison | pandas killed at 4 GB, Spark finishes |

I partition by pickup date, not by timestamp (millions of tiny folders) and not by passenger count (921,181 nulls, one folder holds most trips, and no query filters on it).

## Spark vs pandas

Both tools compute trips and revenue per pickup zone, reversals excluded, over 5 monthly files (May to September): 18,597,780 trips, 3 of 24 columns read, same container, 3 runs each. Both return identical results (18,597,780 trips, $574,906,761.04).

| Tool | Run 1 | Run 2 | Run 3 | Median |
|---|---|---|---|---|
| pandas | 2.6s | 12.0s | 2.3s | 2.6s |
| Spark | 11.5s | 1.0s | 1.0s | 1.0s |

Warm, Spark was about 2.5x faster (1.0s against 2.6s median). On the first run pandas was about 4x faster (2.6s against 11.5s), because Spark pays for JVM start-up and file listing once. pandas run 2 (12.0s) is an outlier I could not explain. This test does not show where pandas breaks: the data fits in memory, so it is not a test of scale.

### Memory test (4 GB cap)

Same 5 files, but all 24 columns, one tool per run, inside a container capped at 4 GB (`spark-4g`, swap disabled).

| Run | Result |
|---|---|
| pandas, 4 GB cap | Killed (exit 137, out of memory) |
| pandas, no cap | 28.7s, peak 6,618 MB (the loaded frame alone is 2,815 MB) |
| Spark, 4 GB cap | 15.7s, same results (18,597,780 trips, $574,906,761.04) |

Spark finished under the cap because it reads only the columns the query uses: the plan's `ReadSchema` has 3 of 24 columns (column pruning). pandas loaded all 24 only because I did not pass `columns=`, and on 3 columns it ran in about 2 seconds in the Day 16 benchmark. So the lesson is that Spark prunes columns for you and pandas needs them selected explicitly. The extra pandas memory is likely copies during `concat` and the filter, which I did not measure.

## Open questions

- `request_source` is filled for almost all rows without a passenger count (codes such as HV0003 and HV0005). I could not confirm which companies these codes are, or why these rows appear in a yellow-taxi file.
- Whether each negative (reversal) row has a matching original positive trip in the file.
- Zone 79: null-passenger rows are 41.8% of its trips (27.3% elsewhere) and explain its high average miles. They do not explain its low fares: those trips earn $1.82 per mile against $3.71 for null-passenger trips elsewhere. Untested why.

## How to run

### Prerequisites
- Docker Desktop (running)
- Python 3.13
- Git

### 1. Clone and set up Python
```powershell
git clone https://github.com/Farid22311/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure credentials
```powershell
Copy-Item .env.example .env
```
Open .env and set POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB to values of your choice. Never commit .env.

### 3. Download the data
Create the folder and download the file:
```powershell
mkdir data
```
From the [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), download the **Yellow Taxi Trip Records** Parquet file for **August 2026** and save it as `data/yellow_tripdata_2026-08.parquet`.

### 4. Start Postgres
```powershell
docker compose --env-file .env -f docker/docker-compose.yml up -d
docker ps
```
You should see that the docker-postgres is running. Postgres is published on port **5433**, not 5432.

### 5. Create the staging table
```powershell
Get-Content sql\create_staging.sql | docker exec -i docker-postgres-1 psql -U <user> -d <db>
```

### 6. Load the data (local)
```powershell
python scripts\load_data.py
```
It takes about 30 seconds and prints the connection settings and the number of rows loaded, which should reach 3,336,716.

### 6b. Load the data (in a container)
Alternative to step 6. Postgres must be running and the staging table must exist.
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm loader
```
The container reaches Postgres by the service name `postgres` on port 5432 (5433 is only the host port). Always pass `--env-file .env`, otherwise Compose recreates Postgres with blank credentials.

### 7. Verify
```powershell
docker exec -it docker-postgres-1 psql -U <user> -d <db> -c "SELECT COUNT(*) FROM staging_yellow_trips;"
```
Expected: `3336716`. Running the loader again keeps the count at 3,336,716, because the script truncates the table first.

### 8. Create and load the clean table
```powershell
Get-Content sql\create_clean.sql | docker exec -i docker-postgres-1 psql -U <user> -d <db>
Get-Content sql\alter_clean.sql  | docker exec -i docker-postgres-1 psql -U <user> -d <db>
Get-Content sql\load_clean.sql   | docker exec -i docker-postgres-1 psql -U <user> -d <db>
```
`load_clean.sql` can be rerun on its own: it truncates the table and restarts `trip_id` inside one transaction.

### 9. Verify the clean table
```powershell
docker exec -it docker-postgres-1 psql -U <user> -d <db> -c "SELECT COUNT(*) FROM clean_yellow_trips;"
docker exec -it docker-postgres-1 psql -U <user> -d <db> -c "SELECT is_reversal, COUNT(*) FROM clean_yellow_trips GROUP BY 1;"
docker exec -it docker-postgres-1 psql -U <user> -d <db> -c "SELECT has_passenger_count, COUNT(*) FROM clean_yellow_trips GROUP BY 1;"
```
Expected: 3,336,715 rows in total, 14,510 with `is_reversal` true, 921,181 with `has_passenger_count` false.

### 10. Run the analytical queries
```powershell
Get-Content sql\queries\q01_<name>.sql | docker exec -i docker-postgres-1 psql -U <user> -d <db>
```
Replace the file name for each query in `sql/queries/`.

### 11. Build the Spark image
```powershell
docker compose --env-file .env -f docker/docker-compose.yml build spark
```

### 12. Clean with Spark
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm spark python spark_jobs/01_clean.py
```
Expected: 3,336,715 rows, 14,510 reversals, 921,181 without passenger count.

### 13. Aggregate and partition
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm spark python spark_jobs/02_aggregate.py
```
Writes `output/trips_by_date/` with one folder per pickup date.

### 14. Benchmark
Put several monthly files in `data/` (`yellow_tripdata_2026-0X.parquet`), then:
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm spark python spark_jobs/03_benchmark.py
```
### 15. Memory test
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm spark-4g python spark_jobs/04_memory_test.py pandas
docker compose --env-file .env -f docker/docker-compose.yml run --rm spark-4g python spark_jobs/04_memory_test.py spark
```
Exit code 137 for pandas means the container ran out of memory, which is the expected result.

### 16. Airflow
Airflow runs from its own folder and its own Compose project, separate from Postgres and Spark. The `airflow/docker-compose.yaml` is the official file for Airflow 3.3.0, with `AIRFLOW__CORE__LOAD_EXAMPLES` set to `'false'`.

Run everything from inside `airflow/`, so that `dags/` sits next to the compose file (a nested `airflow/airflow/dags` folder is not mounted and the DAG is never found):
```powershell
cd airflow
mkdir dags, logs, plugins, config
Set-Content -Path .env -Value "AIRFLOW_UID=50000" -Encoding ascii
docker compose up airflow-init
docker compose up -d
docker compose exec airflow-scheduler airflow dags list
```
Open http://localhost:8080 and log in as `airflow` / `airflow`. New DAGs start paused: switch `hello_dag` on, then trigger it. Expected: three green tasks (`start`, `hello`, `end`).

If a DAG file you add does not show up, the dag-processor rescans the folder only every few minutes. Run `docker compose restart airflow-dag-processor` to force it.

The compose file mounts two folders into the Airflow containers: `airflow/dags` and `../data` (the repo's `data/` folder). Both lines go under `volumes:`:
```
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/../data:/opt/airflow/data
```
Check both mounts from inside a container (`dags` must show the DAG files and `data` the Parquet files):
```powershell
docker compose exec airflow-scheduler ls /opt/airflow/dags
docker compose exec airflow-scheduler ls /opt/airflow/data
```

Trigger `taxi_download` in the UI and enter a `month` such as `2026-08`. The file lands in `data/downloads/`, not in `data/`, so the Spark and benchmark file patterns still match only the original files. Running the same month twice skips the download (the file already exists), and a download is written to a `.part` file and renamed only when complete.

Stop with `docker compose down`. Do not add `--volumes` unless you want to delete Airflow's database and run history.