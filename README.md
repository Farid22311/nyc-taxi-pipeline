# nyc-taxi-pipeline

It’s an end-to-end data pipeline on NYC TLC Yellow Taxi trip data. Making this project to learn and demonstrate core data engineering tools.

## Data

NYC TLC Yellow Taxi trips, monthly Parquet files, free and public, the files are not stored in the repo.

## Planned stack

* Docker / Compose: runs the services (Postgres, Airflow), so the setup works the same on any machine.
* Postgres: the database where the data is stored and queried with SQL.
* PySpark: processes large amounts of data, used to clean and aggregate the data.
* Airflow: schedules and runs the pipeline steps automatically.

## Status

   - Week 1: Postgres running via Docker Compose, raw August 2026 file loaded into staging (done)
   - Week 2: clean table designed and loaded with documented cleaning rules (done); next: analytical queries and the loader in a container

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

### 6. Load the data
```powershell
python scripts\load_data.py
```
It takes about 30 seconds and prints the connection settings and the number of rows loaded, which should reach 3,336,716.

### 7. Verify
```powershell
docker exec -it docker-postgres-1 psql -U <user> -d <db> -c "SELECT COUNT(*) FROM staging_yellow_trips;"
```
Expected: `3336716`. Running the loader again keeps the count at 3,336,716, because the script truncates the table first.

### 8. Create and load the clean table
Get-Content sql\create_clean.sql | docker exec -i docker-postgres-1 psql -U <user> -d <db>
Get-Content sql\alter_clean.sql  | docker exec -i docker-postgres-1 psql -U <user> -d <db>
Get-Content sql\load_clean.sql   | docker exec -i docker-postgres-1 psql -U <user> -d <db>

### 9. Verify the clean table
SELECT COUNT(*) FROM clean_yellow_trips;                                  -- 3,336,715
SELECT is_reversal, COUNT(*) FROM clean_yellow_trips GROUP BY 1;          -- 14,510 true
SELECT has_passenger_count, COUNT(*) FROM clean_yellow_trips GROUP BY 1;  -- 921,181 false
