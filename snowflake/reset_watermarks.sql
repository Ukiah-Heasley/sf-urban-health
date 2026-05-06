-- Resets all ingest watermarks to 2026-05-01.
-- The next DAG run will fetch everything from that date forward.
USE DATABASE SF_URBAN_HEALTH;

MERGE INTO METADATA.INGEST_WATERMARKS AS t
USING (
    SELECT column1 AS dataset_name, '2026-05-01'::TIMESTAMP_NTZ AS watermark
    FROM VALUES ('permits'), ('evictions'), ('incidents')
) AS s ON t.dataset_name = s.dataset_name
WHEN MATCHED THEN
    UPDATE SET
        t.watermark   = s.watermark,
        t.updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (dataset_name, watermark, updated_at)
    VALUES (s.dataset_name, s.watermark, CURRENT_TIMESTAMP());

-- Verify
SELECT dataset_name, watermark, updated_at
FROM METADATA.INGEST_WATERMARKS
ORDER BY dataset_name;
