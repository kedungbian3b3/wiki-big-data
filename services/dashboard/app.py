from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from datetime import datetime, timezone

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from common.config import DashboardConfig, PostgresConfig


st.set_page_config(
    page_title="Wiki Big Data Streaming Lab",
    page_icon="📡",
    layout="wide",
)

dashboard_config = DashboardConfig()
pg_config = PostgresConfig()


def query(sql: str, params=None) -> pd.DataFrame:
    with psycopg2.connect(pg_config.dsn()) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def safe_metric_value(value, default="0"):
    if value is None or pd.isna(value):
        return default
    return value


st_autorefresh(interval=dashboard_config.refresh_ms, key="dashboard_refresh")

st.title("📡 Wiki Big Data Streaming Lab")
st.caption(
    "Realtime Wikimedia RecentChange → Kafka-compatible Redpanda → Stream Processor → PostgreSQL → Streamlit"
)

try:
    metrics = query(
        """
        SELECT
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE event_time >= now() - interval '1 minute') AS events_last_minute,
            COUNT(*) FILTER (WHERE event_time >= now() - interval '5 minutes') AS events_last_5_minutes,
            COUNT(*) FILTER (WHERE is_bot IS TRUE AND event_time >= now() - interval '5 minutes') AS bot_events_5m,
            COUNT(*) FILTER (WHERE is_bot IS FALSE AND event_time >= now() - interval '5 minutes') AS human_events_5m,
            MAX(event_time) AS latest_event_time,
            MAX(ingest_time) AS latest_ingest_time
        FROM fact_recent_changes;
        """
    ).iloc[0]

    total_5m = int(metrics["events_last_5_minutes"] or 0)
    bot_5m = int(metrics["bot_events_5m"] or 0)
    bot_pct = round((bot_5m / total_5m) * 100, 2) if total_5m else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total events", f"{int(metrics['total_events'] or 0):,}")
    c2.metric("Events last 1 min", f"{int(metrics['events_last_minute'] or 0):,}")
    c3.metric("Events last 5 min", f"{total_5m:,}")
    c4.metric("Bot % last 5 min", f"{bot_pct}%")
    c5.metric(
        "Latest event UTC",
        str(safe_metric_value(metrics["latest_event_time"], "No data yet"))[:19],
    )

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Events timeline - last 10 minutes")
        timeline = query(
            """
            SELECT
                date_bin('10 seconds', event_time, TIMESTAMPTZ '2000-01-01') AS bucket,
                COUNT(*) AS events
            FROM fact_recent_changes
            WHERE event_time >= now() - interval '10 minutes'
            GROUP BY 1
            ORDER BY 1;
            """
        )

        if timeline.empty:
            st.info("Chưa có data. Chờ 10-30 giây hoặc xem log producer/processor.")
        else:
            timeline = timeline.set_index("bucket")
            st.line_chart(timeline)

    with right:
        st.subheader("Bot vs Human - last 5 minutes")
        bot_human = pd.DataFrame(
            {
                "type": ["bot", "human"],
                "events": [
                    int(metrics["bot_events_5m"] or 0),
                    int(metrics["human_events_5m"] or 0),
                ],
            }
        ).set_index("type")
        st.bar_chart(bot_human)

    left2, right2 = st.columns(2)

    with left2:
        st.subheader("Top wikis - last 10 minutes")
        top_wikis = query(
            """
            SELECT COALESCE(wiki, 'unknown') AS wiki, COUNT(*) AS events
            FROM fact_recent_changes
            WHERE event_time >= now() - interval '10 minutes'
            GROUP BY 1
            ORDER BY events DESC
            LIMIT 15;
            """
        )
        if top_wikis.empty:
            st.info("No wiki data yet.")
        else:
            st.bar_chart(top_wikis.set_index("wiki"))

    with right2:
        st.subheader("Top change types - last 10 minutes")
        change_types = query(
            """
            SELECT COALESCE(change_type, 'unknown') AS change_type, COUNT(*) AS events
            FROM fact_recent_changes
            WHERE event_time >= now() - interval '10 minutes'
            GROUP BY 1
            ORDER BY events DESC
            LIMIT 10;
            """
        )
        if change_types.empty:
            st.info("No change type data yet.")
        else:
            st.bar_chart(change_types.set_index("change_type"))

    st.subheader("Latest realtime events")
    latest = query(
        """
        SELECT
            event_time,
            wiki,
            user_name,
            change_type,
            is_bot,
            bytes_delta,
            title,
            page_url
        FROM fact_recent_changes
        ORDER BY event_time DESC
        LIMIT 30;
        """
    )
    st.dataframe(latest, use_container_width=True, hide_index=True)

    st.caption(
        f"Dashboard refresh: {dashboard_config.refresh_ms} ms | "
        f"Rendered at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )

except Exception as exc:
    st.error("Dashboard chưa query được PostgreSQL.")
    st.exception(exc)
    st.info(
        "Kiểm tra bằng lệnh: docker compose logs -f postgres processor producer"
    )
