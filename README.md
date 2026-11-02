# nyc-taxi-pipeline
it’s an end-to-end data pipeline on NYC TLC Yellow Taxi trip data.
## Data
NYC TLC Yellow Taxi trips, monthly Parquet files, free and public, the files are not stored in the repo.
## Planned stack
Docker / Compose: runs the services (Postgres, later Airflow), so the setup works the same on any machine.
Postgres: the database where the data is stored and queried with SQL.
PySpark: processes large amounts of data in parallel, used for cleaning and aggregating the trips.
Airflow: schedules and runs the pipeline steps automatically, with retries.
## Status
Week 1 of 9: Finished day 1 of planning and scheduling the data
## How to run
Coming soon.
