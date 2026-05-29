-- One-time Snowflake bootstrap for the SF Urban Health pipeline.
-- Run once in a worksheet (or `snow sql -f snowflake/bootstrap.sql`) with a role
-- that can create databases (e.g. SYSADMIN). Idempotent — safe to re-run.
--
-- Fill in the S3 stage credentials below before running the CREATE STAGE block,
-- or create the stage via a storage integration (preferred for production).

CREATE DATABASE IF NOT EXISTS SF_URBAN_HEALTH;

CREATE SCHEMA IF NOT EXISTS SF_URBAN_HEALTH.RAW;
CREATE SCHEMA IF NOT EXISTS SF_URBAN_HEALTH.STAGING;
CREATE SCHEMA IF NOT EXISTS SF_URBAN_HEALTH.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS SF_URBAN_HEALTH.MARTS;
CREATE SCHEMA IF NOT EXISTS SF_URBAN_HEALTH.METADATA;

-- External stage over the S3 raw lake. Replace the credentials (or swap for a
-- STORAGE INTEGRATION) and point URL at your bucket.
CREATE STAGE IF NOT EXISTS SF_URBAN_HEALTH.RAW.S3_STAGE
  URL = 's3://sf-urban-health/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = JSON);

-- Raw landing tables — one VARIANT payload per record (NDJSON from S3).
CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.RAW.PERMITS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.RAW.EVICTIONS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.RAW.INCIDENTS (
    payload    VARIANT,
    _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Per-dataset high-water mark (read at the start of each ingest, MERGE'd after load).
CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.METADATA.INGEST_WATERMARKS (
    dataset_name  VARCHAR       NOT NULL,
    watermark     TIMESTAMP_NTZ NOT NULL,
    updated_at    TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT pk_ingest_watermarks PRIMARY KEY (dataset_name)
);

-- Pipeline observability tables (written by the ingest_pipeline_metadata DAG).
CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.METADATA.AIRFLOW_DAG_RUNS (
    dag_id           VARCHAR       NOT NULL,
    run_id           VARCHAR       NOT NULL,
    state            VARCHAR,
    execution_date   TIMESTAMP_NTZ,
    start_date       TIMESTAMP_NTZ,
    end_date         TIMESTAMP_NTZ,
    duration_seconds FLOAT,
    run_type         VARCHAR,
    _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (dag_id, run_id)
);

CREATE TABLE IF NOT EXISTS SF_URBAN_HEALTH.METADATA.AIRFLOW_TASK_INSTANCES (
    dag_id             VARCHAR       NOT NULL,
    run_id             VARCHAR       NOT NULL,
    task_id            VARCHAR       NOT NULL,
    state              VARCHAR,
    start_date         TIMESTAMP_NTZ,
    end_date           TIMESTAMP_NTZ,
    duration_seconds   FLOAT,
    try_number         INTEGER,
    records_fetched    INTEGER,
    max_watermark      TIMESTAMP_NTZ,
    s3_path            VARCHAR,
    _loaded_at         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (dag_id, run_id, task_id)
);

-- Optional: a read-only role for a hosted dashboard / BI tool. Uncomment and
-- set a password (or use key-pair auth) if you deploy the live Dash app.
-- CREATE ROLE IF NOT EXISTS SF_URBAN_HEALTH_READER;
-- GRANT USAGE ON DATABASE SF_URBAN_HEALTH TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT USAGE ON SCHEMA SF_URBAN_HEALTH.MARTS TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT USAGE ON SCHEMA SF_URBAN_HEALTH.METADATA TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT SELECT ON ALL TABLES IN SCHEMA SF_URBAN_HEALTH.MARTS TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA SF_URBAN_HEALTH.MARTS TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT SELECT ON ALL TABLES IN SCHEMA SF_URBAN_HEALTH.METADATA TO ROLE SF_URBAN_HEALTH_READER;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA SF_URBAN_HEALTH.METADATA TO ROLE SF_URBAN_HEALTH_READER;
