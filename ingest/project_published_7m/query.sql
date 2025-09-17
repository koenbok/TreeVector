
    SELECT * 
    FROM `framer-raw-data.raw_logs_production.project_published`
    WHERE TIMESTAMP_MILLIS(data.timestamp) > TIMESTAMP("2024-07-01 00:00:00 UTC")