CREATE TABLE IF NOT EXISTS fact_recent_changes (
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL DEFAULT now(),

    wiki TEXT,
    domain TEXT,
    server_name TEXT,
    user_name TEXT,
    title TEXT,
    change_type TEXT,
    namespace_id INTEGER,

    is_bot BOOLEAN,
    is_minor BOOLEAN,

    length_old INTEGER,
    length_new INTEGER,
    bytes_delta INTEGER,

    rev_old BIGINT,
    rev_new BIGINT,

    page_url TEXT,
    comment TEXT,

    raw_event JSONB
);

CREATE INDEX IF NOT EXISTS idx_fact_recent_changes_event_time
    ON fact_recent_changes (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_recent_changes_wiki_event_time
    ON fact_recent_changes (wiki, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_recent_changes_bot_event_time
    ON fact_recent_changes (is_bot, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_recent_changes_change_type_event_time
    ON fact_recent_changes (change_type, event_time DESC);

CREATE OR REPLACE VIEW v_recent_change_minute AS
SELECT
    date_trunc('minute', event_time) AS minute_window,
    wiki,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE is_bot IS TRUE) AS bot_events,
    COUNT(*) FILTER (WHERE is_bot IS FALSE) AS human_events,
    COALESCE(SUM(bytes_delta), 0) AS bytes_delta_sum,
    COUNT(DISTINCT user_name) AS unique_users
FROM fact_recent_changes
GROUP BY 1, 2;
