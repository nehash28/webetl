-- Runs once, the first time the Postgres volume is created.
-- The `warehouse` database and `webetl` user come from POSTGRES_DB / POSTGRES_USER.
-- Airflow keeps its own metadata in a separate database.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
