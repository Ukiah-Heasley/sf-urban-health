COPY INTO {{ params.database }}.{{ params.table }} (payload)
FROM @{{ params.database }}.RAW.S3_STAGE/raw/{{ params.name }}/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;
