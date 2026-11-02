# nyc-taxi-pipeline
it’s an end-to-end data pipeline on NYC TLC Yellow Taxi trip data. Making this project to learn and demonstrate the core of data engineering
## Data
NYC TLC Yellow Taxi trips, monthly Parquet files, free and public, the files are not stored in the repo. 
## Planned stack
- Docker / Compose: runs the services (Postgres,Airflow), so the setup works the same on any machine.
- Postgres: the database where the data is stored and queried with SQL.
- PySpark: processes large amounts of data, used to clean and aggregate the data.
- Airflow: schedules and runs the pipeline steps automatically.
## Status
Week 1 of 9: repo and project structure set up. Next: Docker and Postgres.
## How to run
Coming soon.
