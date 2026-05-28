-- Jinja-templated rather than driver-bound because every placeholder is
-- a SQL identifier or stage path component (database, table, stage path),
-- not a value — Snowflake bind parameters cannot bind identifiers. All
-- inputs come from `make_ingest_dag(DagConfig(...))` callers, never from
-- user-controlled data, so there is no injection surface.
COPY INTO {{ params.database }}.{{ params.table }} (payload)
FROM @{{ params.database }}.RAW.S3_STAGE/raw/{{ params.name }}/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
