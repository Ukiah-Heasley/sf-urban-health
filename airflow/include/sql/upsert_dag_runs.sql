MERGE INTO SF_URBAN_HEALTH.METADATA.AIRFLOW_DAG_RUNS AS t
USING (
    SELECT %(dag_id)s                        AS dag_id,
           %(run_id)s                        AS run_id,
           %(state)s                         AS state,
           %(execution_date)s::TIMESTAMP_NTZ AS execution_date,
           %(start_date)s::TIMESTAMP_NTZ     AS start_date,
           %(end_date)s::TIMESTAMP_NTZ       AS end_date,
           %(duration_seconds)s              AS duration_seconds,
           %(run_type)s                      AS run_type
) AS s ON t.dag_id = s.dag_id AND t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET
    state            = s.state,
    start_date       = s.start_date,
    end_date         = s.end_date,
    duration_seconds = s.duration_seconds
WHEN NOT MATCHED THEN INSERT
    (dag_id, run_id, state, execution_date, start_date, end_date, duration_seconds, run_type)
VALUES
    (s.dag_id, s.run_id, s.state, s.execution_date, s.start_date, s.end_date,
     s.duration_seconds, s.run_type);
