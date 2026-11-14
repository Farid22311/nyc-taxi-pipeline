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

## Repo structure

```
docker/          docker-compose.yml (Postgres, loader), Dockerfile.loader
scripts/         load_data.py (Parquet -> staging table)
sql/             create_staging, create_clean, alter_clean, load_clean
sql/checks/      one-off queries I used to investigate the data and decide the cleaning rules
sql/queries/     analytical queries q01 to q06
data/            local Parquet files (not committed)
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
-- Conclusion: Aug 20 is mainly a spike in trips (+30%, then back below the Aug 19 level on Aug 21).
-- Revenue per trip rose 8% and did not revert (29.40 -> 31.79 -> 31.29), so that part is a step up.
-- Trips explain most of the +1,249,617.47 revenue jump.

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
### 6b. Load the data (in a container)
Alternative to step 6. Postgres must be running and the staging table must exist.
```powershell
docker compose --env-file .env -f docker/docker-compose.yml run --rm loader
```
The container reaches Postgres by the service name `postgres` on port 5432 (5433 is only the host port). Always pass `--env-file .env`, otherwise Compose recreates Postgres with blank credentials.

It takes about 30 seconds and prints the connection settings and the number of rows loaded, which should reach 3,336,716.

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
