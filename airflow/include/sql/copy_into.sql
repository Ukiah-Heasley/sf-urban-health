-- Jinja-templated rather than driver-bound because every placeholder is
-- a SQL identifier or stage path component (database, table, stage path),
-- not a value — Snowflake bind parameters cannot bind identifiers. All
-- inputs come from `make_ingest_dag(DagConfig(...))` callers, never from
-- user-controlled data, so there is no injection surface.
COPY INTO {{ params.database }}.{{ params.table }} (payload)
FROM @{{ params.database }}.RAW.S3_STAGE/{{ ti.xcom_pull(task_ids=params.extract_task_id, key='raw_key') }}
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
