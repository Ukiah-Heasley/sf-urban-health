MERGE INTO SF_URBAN_HEALTH.METADATA.AIRFLOW_TASK_INSTANCES AS t
USING (
    SELECT %(dag_id)s                       AS dag_id,
           %(run_id)s                       AS run_id,
           %(task_id)s                      AS task_id,
           %(state)s                        AS state,
           %(start_date)s::TIMESTAMP_NTZ    AS start_date,
           %(end_date)s::TIMESTAMP_NTZ      AS end_date,
           %(duration_seconds)s             AS duration_seconds,
           %(try_number)s                   AS try_number,
           %(records_fetched)s              AS records_fetched,
           %(max_watermark)s::TIMESTAMP_NTZ AS max_watermark,
           %(s3_path)s                      AS s3_path
) AS s ON t.dag_id = s.dag_id AND t.run_id = s.run_id AND t.task_id = s.task_id
WHEN MATCHED THEN UPDATE SET
    state            = s.state,
    start_date       = s.start_date,
    end_date         = s.end_date,
    duration_seconds = s.duration_seconds,
    try_number       = s.try_number,
    records_fetched  = s.records_fetched,
    max_watermark    = s.max_watermark,
    s3_path          = s.s3_path
WHEN NOT MATCHED THEN INSERT
    (dag_id, run_id, task_id, state, start_date, end_date, duration_seconds,
     try_number, records_fetched, max_watermark, s3_path)
VALUES
    (s.dag_id, s.run_id, s.task_id, s.state, s.start_date, s.end_date,
     s.duration_seconds, s.try_number, s.records_fetched, s.max_watermark, s.s3_path);
