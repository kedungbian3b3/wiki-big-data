SELECT
    COUNT(*) AS total_events,
    MAX(event_time) AS latest_event_time,
    MAX(ingest_time) AS latest_ingest_time
FROM fact_recent_changes;

SELECT
    date_trunc('minute', event_time) AS minute_window,
    COUNT(*) AS total_events
FROM fact_recent_changes
GROUP BY 1
ORDER BY 1 DESC
LIMIT 10;

SELECT
    wiki,
    COUNT(*) AS total_events
FROM fact_recent_changes
WHERE event_time >= now() - interval '10 minutes'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
