-- Bound via SQLExecuteQueryOperator.parameters: %(name)s is the dataset
-- name, %(watermark)s is the ISO-8601 timestamp pulled from XCom at task
-- execution time. Both are values (not SQL identifiers), so they bind
-- cleanly through the Snowflake driver — no Jinja string concatenation.
MERGE INTO METADATA.INGEST_WATERMARKS AS t
USING (
    SELECT %(name)s                      AS dataset_name,
           %(watermark)s::TIMESTAMP_NTZ  AS watermark
) AS s
ON t.dataset_name = s.dataset_name
WHEN MATCHED THEN
    UPDATE SET t.watermark = s.watermark, t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (dataset_name, watermark, updated_at)
    VALUES (s.dataset_name, s.watermark, CURRENT_TIMESTAMP());
