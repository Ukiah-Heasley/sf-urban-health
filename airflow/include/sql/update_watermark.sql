MERGE INTO METADATA.INGEST_WATERMARKS AS t
USING (
    SELECT
        '{{ params.name }}' AS dataset_name,
        '{{ ti.xcom_pull(task_ids=params.extract_task_id, key="max_watermark") }}'::TIMESTAMP_NTZ AS watermark
) AS s
ON t.dataset_name = s.dataset_name
WHEN MATCHED THEN
    UPDATE SET t.watermark = s.watermark, t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (dataset_name, watermark, updated_at)
    VALUES (s.dataset_name, s.watermark, CURRENT_TIMESTAMP());
