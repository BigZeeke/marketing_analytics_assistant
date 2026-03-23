"""
monitor.py
==========
Marketing Intelligence Monitor
Runs diagnostic queries against marketing_analytics.db,
sends a metrics snapshot to Claude, and writes structured
recommendations back to a new `recommendations` table.

Usage:
    python monitor.py              # run once
    python monitor.py --dry-run    # print output, don't write to DB
"""

import sqlite3
import json
import argparse
import os
import tomllib
from datetime import datetime, date, timezone
import anthropic

# ─── CONFIG ──────────────────────────────────────────────────────────────────

DB_PATH = "marketing_analytics.db"
MODEL   = "claude-sonnet-4-6"

# ─── API KEY ─────────────────────────────────────────────────────────────────

def load_api_key() -> str:
    """
    Load the Anthropic API key safely — never hardcoded.
    Priority:
      1. ANTHROPIC_API_KEY environment variable (CI, cron, production)
      2. .streamlit/secrets.toml (local development)
    """
    key = os.environ.get("ANTHROPIC_API_KEY")
    if key:
        return key
    try:
        with open(".streamlit/secrets.toml", "rb") as f:
            secrets = tomllib.load(f)
        return secrets["anthropic"]["api_key"]
    except FileNotFoundError:
        raise RuntimeError(
            "Could not find .streamlit/secrets.toml. "
            "Set ANTHROPIC_API_KEY as an environment variable or add it to secrets.toml."
        )
    except KeyError:
        raise RuntimeError(
            "api_key not found under [anthropic] in .streamlit/secrets.toml. "
            "Make sure it looks like:\n\n[anthropic]\napi_key = \"sk-ant-...\""
        )

# ─── DATABASE SETUP ──────────────────────────────────────────────────────────

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_recommendations_table(conn):
    """Create recommendations table if it doesn't exist yet."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recommendations (
            rec_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT NOT NULL,
            channel         TEXT,
            metric          TEXT,
            current_value   TEXT,
            action          TEXT NOT NULL,
            rationale       TEXT NOT NULL,
            priority        TEXT NOT NULL,   -- high / medium / low
            confidence      REAL,            -- 0.0 – 1.0
            simulated_impact TEXT,
            status          TEXT DEFAULT 'pending'  -- pending / applied / dismissed
        )
    """)
    conn.commit()


# ─── DIAGNOSTIC QUERIES ───────────────────────────────────────────────────────

def run_diagnostics(conn):
    """
    Run a focused set of diagnostic queries and return
    a dict of labelled result sets.
    """
    diagnostics = {}

    # 1. ROAS by channel (last 30 days of active/completed campaigns)
    rows = conn.execute("""
        SELECT
            c.channel,
            ROUND(SUM(c.spend), 2)                          AS total_spend,
            ROUND(SUM(o.amount), 2)                         AS total_revenue,
            CASE WHEN SUM(c.spend) > 0
                 THEN ROUND(SUM(o.amount) / SUM(c.spend), 2)
                 ELSE NULL END                              AS roas
        FROM campaigns c
        LEFT JOIN orders o ON o.campaign_id = c.campaign_id
        WHERE c.status IN ('active', 'completed')
        GROUP BY c.channel
        ORDER BY roas DESC NULLS LAST
    """).fetchall()
    diagnostics["roas_by_channel"] = [dict(r) for r in rows]

    # 2. CPA by channel
    rows = conn.execute("""
        SELECT
            c.channel,
            COUNT(l.lead_id)                                AS total_leads,
            SUM(CASE WHEN l.status = 'converted' THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(c.spend), 2)                          AS total_spend,
            CASE WHEN SUM(CASE WHEN l.status = 'converted' THEN 1 ELSE 0 END) > 0
                 THEN ROUND(SUM(c.spend) /
                      SUM(CASE WHEN l.status = 'converted' THEN 1 ELSE 0 END), 2)
                 ELSE NULL END                              AS cpa
        FROM campaigns c
        LEFT JOIN leads l ON l.campaign_id = c.campaign_id
        WHERE c.status IN ('active', 'completed')
        GROUP BY c.channel
        ORDER BY cpa ASC NULLS LAST
    """).fetchall()
    diagnostics["cpa_by_channel"] = [dict(r) for r in rows]

    # 3. Email engagement by campaign type
    rows = conn.execute("""
        SELECT
            ec.email_type,
            COUNT(DISTINCT ec.email_campaign_id)            AS campaigns_sent,
            COUNT(CASE WHEN ee.event_type = 'opened'    THEN 1 END) AS opens,
            COUNT(CASE WHEN ee.event_type = 'clicked'   THEN 1 END) AS clicks,
            COUNT(CASE WHEN ee.event_type = 'converted' THEN 1 END) AS conversions,
            COUNT(CASE WHEN ee.event_type = 'unsubscribed' THEN 1 END) AS unsubs,
            ROUND(
                100.0 * COUNT(CASE WHEN ee.event_type = 'opened' THEN 1 END)
                / NULLIF(SUM(ec.list_size), 0), 2
            )                                               AS open_rate_pct
        FROM email_campaigns ec
        LEFT JOIN email_events ee ON ee.email_campaign_id = ec.email_campaign_id
        GROUP BY ec.email_type
        ORDER BY open_rate_pct DESC NULLS LAST
    """).fetchall()
    diagnostics["email_engagement"] = [dict(r) for r in rows]

    # 4. Lead velocity — new leads per channel (last 60 days)
    rows = conn.execute("""
        SELECT
            c.channel,
            COUNT(l.lead_id)                                AS new_leads,
            ROUND(AVG(l.score), 1)                          AS avg_lead_score,
            SUM(CASE WHEN l.status = 'converted' THEN 1 ELSE 0 END) AS converted,
            ROUND(
                100.0 * SUM(CASE WHEN l.status = 'converted' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(l.lead_id), 0), 1
            )                                               AS conversion_rate_pct
        FROM leads l
        JOIN campaigns c ON c.campaign_id = l.campaign_id
        WHERE l.created_at >= date('now', '-60 days')
        GROUP BY c.channel
        ORDER BY new_leads DESC
    """).fetchall()
    diagnostics["lead_velocity"] = [dict(r) for r in rows]

    # 5. Top 5 and bottom 5 ads by ROAS
    rows = conn.execute("""
        SELECT
            a.ad_id,
            a.headline_1,
            ag.ad_group_name,
            ROUND(SUM(ap.spend), 2)            AS total_spend,
            ROUND(SUM(ap.conversion_value), 2) AS total_conv_value,
            CASE WHEN SUM(ap.spend) > 0
                 THEN ROUND(SUM(ap.conversion_value) / SUM(ap.spend), 2)
                 ELSE NULL END                 AS roas,
            ROUND(AVG(ap.quality_score), 1)    AS avg_quality_score
        FROM ads a
        JOIN ad_groups ag ON ag.ad_group_id = a.ad_group_id
        JOIN ad_performance ap ON ap.ad_id = a.ad_id
        GROUP BY a.ad_id, a.headline_1, ag.ad_group_name
        HAVING SUM(ap.spend) > 0
        ORDER BY roas DESC
        LIMIT 5
    """).fetchall()
    diagnostics["top_ads"] = [dict(r) for r in rows]

    rows = conn.execute("""
        SELECT
            a.ad_id,
            a.headline_1,
            ag.ad_group_name,
            ROUND(SUM(ap.spend), 2)            AS total_spend,
            ROUND(SUM(ap.conversion_value), 2) AS total_conv_value,
            CASE WHEN SUM(ap.spend) > 0
                 THEN ROUND(SUM(ap.conversion_value) / SUM(ap.spend), 2)
                 ELSE NULL END                 AS roas,
            ROUND(AVG(ap.quality_score), 1)    AS avg_quality_score
        FROM ads a
        JOIN ad_groups ag ON ag.ad_group_id = a.ad_group_id
        JOIN ad_performance ap ON ap.ad_id = a.ad_id
        GROUP BY a.ad_id, a.headline_1, ag.ad_group_name
        HAVING SUM(ap.spend) > 0
        ORDER BY roas ASC
        LIMIT 5
    """).fetchall()
    diagnostics["bottom_ads"] = [dict(r) for r in rows]

    # 6. Revenue by campaign type
    rows = conn.execute("""
        SELECT
            c.campaign_type,
            COUNT(DISTINCT c.campaign_id)   AS campaigns,
            ROUND(SUM(c.spend), 2)          AS total_spend,
            ROUND(SUM(o.amount), 2)         AS total_revenue,
            CASE WHEN SUM(c.spend) > 0
                 THEN ROUND(SUM(o.amount) / SUM(c.spend), 2)
                 ELSE NULL END              AS roas
        FROM campaigns c
        LEFT JOIN orders o ON o.campaign_id = c.campaign_id
        GROUP BY c.campaign_type
        ORDER BY roas DESC NULLS LAST
    """).fetchall()
    diagnostics["revenue_by_campaign_type"] = [dict(r) for r in rows]

    return diagnostics


# ─── CLAUDE RECOMMENDATION CALL ──────────────────────────────────────────────

_BASE_PROMPT = """
You are a senior digital marketing analyst. You receive a JSON snapshot of
marketing performance metrics and return a structured list of prioritized
recommendations. You are rigorous, concise, and data-driven.

Return ONLY a valid JSON array. No markdown, no explanation outside the array.

Each recommendation object must have exactly these fields:
{
  "channel":          string or null,   // e.g. "Paid", "Email", "Organic", null for cross-channel
  "metric":           string or null,   // the primary metric driving this recommendation
  "current_value":    string or null,   // the observed value, as a readable string
  "action":           string,           // what to do, concise imperative (≤ 15 words)
  "rationale":        string,           // why, referencing the data (≤ 40 words)
  "priority":         string,           // "high", "medium", or "low"
  "confidence":       float,            // 0.0 – 1.0
  "simulated_impact": string or null    // estimated outcome if action is taken (≤ 20 words)
}

General rules:
- Return 5 to 8 recommendations total
- Prioritize by potential revenue impact
- Be specific — reference actual numbers from the data
""".strip()

_STANCE_BLOCKS = {
    "conservative": """
RISK STANCE: CONSERVATIVE
- Only flag channels with ROAS < 1.0 as critical underperformers
- Only flag channels with CPA > 3x the best-performing channel as high priority
- Cap all budget change suggestions at 15%
- Prefer monitoring language: use "review", "consider reducing", "monitor closely"
- Only recommend pausing a channel if ROAS < 0.5
- Require confidence > 0.85 before recommending any spend change
- Flag email open rate < 10% as medium priority
""".strip(),

    "neutral": """
RISK STANCE: NEUTRAL
- Flag channels with ROAS < 1.5 as underperforming
- Flag channels with CPA > 2x the best-performing channel as high priority
- Budget change suggestions up to 30%
- Balance action and monitoring language equally
- Standard confidence threshold > 0.75
- Flag email open rate < 15% as medium priority
""".strip(),

    "aggressive": """
RISK STANCE: AGGRESSIVE
- Flag channels with ROAS < 2.0 as underperforming
- Flag channels with CPA > 1.5x the best-performing channel as high priority
- Budget change suggestions up to 50%
- Prefer immediate action language: use "pause immediately", "reallocate now", "cut losses", "scale fast"
- Do not suggest monitoring — recommend decisive action
- Accept confidence threshold > 0.65
- Flag email open rate < 20% as medium priority
- Recommend cutting underperformers entirely before considering creative refresh
""".strip(),
}


def build_system_prompt(stance: str = "neutral") -> str:
    stance_block = _STANCE_BLOCKS.get(stance, _STANCE_BLOCKS["neutral"])
    return f"{_BASE_PROMPT}\n\n{stance_block}"


def get_recommendations(diagnostics: dict, stance: str = "neutral") -> list:
    """Send diagnostics to Claude, return parsed recommendation list."""
    client = anthropic.Anthropic(api_key=load_api_key())

    user_message = f"""
Here is today's marketing performance snapshot. Today's date: {date.today().isoformat()}

{json.dumps(diagnostics, indent=2)}

Return your recommendations as a JSON array.
""".strip()

    response = client.messages.create(
        model=MODEL,
        max_tokens=2500,
        system=build_system_prompt(stance),
        messages=[{"role": "user", "content": user_message}]
    )

    raw = response.content[0].text.strip()

    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    return json.loads(raw)


# ─── WRITE RESULTS ────────────────────────────────────────────────────────────

def write_recommendations(conn, recommendations: list, run_at: str):
    conn.executemany("""
        INSERT INTO recommendations
            (run_at, channel, metric, current_value, action, rationale,
             priority, confidence, simulated_impact, status)
        VALUES
            (:run_at, :channel, :metric, :current_value, :action, :rationale,
             :priority, :confidence, :simulated_impact, 'pending')
    """, [
        {**rec, "run_at": run_at} for rec in recommendations
    ])
    conn.commit()


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main(dry_run: bool = False, stance: str = "neutral"):
    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\n{'='*60}")
    print(f"  Marketing Intelligence Monitor")
    print(f"  Run: {run_at}  |  Stance: {stance.upper()}{'  [DRY RUN]' if dry_run else ''}")
    print(f"{'='*60}\n")

    conn = get_connection()

    if not dry_run:
        ensure_recommendations_table(conn)

    print("⏳ Running diagnostics...")
    diagnostics = run_diagnostics(conn)
    print(f"✅ Diagnostics complete — {len(diagnostics)} metric groups collected\n")

    print(f"🤖 Calling Claude for recommendations ({stance} stance)...")
    recommendations = get_recommendations(diagnostics, stance=stance)
    print(f"✅ {len(recommendations)} recommendations received\n")

    # Pretty print
    print("─" * 60)
    for i, rec in enumerate(recommendations, 1):
        priority_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(rec.get("priority", ""), "⚪")
        print(f"{priority_icon} [{i}] {rec.get('action', '')}")
        print(f"     Channel:    {rec.get('channel') or 'Cross-channel'}")
        print(f"     Metric:     {rec.get('metric') or '—'}  |  Value: {rec.get('current_value') or '—'}")
        print(f"     Rationale:  {rec.get('rationale', '')}")
        print(f"     Impact:     {rec.get('simulated_impact') or '—'}")
        print(f"     Confidence: {rec.get('confidence', 0):.0%}")
        print()

    if dry_run:
        print("ℹ️  Dry run — nothing written to database.")
    else:
        write_recommendations(conn, recommendations, run_at)
        print(f"✅ {len(recommendations)} recommendations written to `recommendations` table.")

    conn.close()
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Marketing Intelligence Monitor")
    parser.add_argument("--dry-run", action="store_true", help="Print output without writing to DB")
    parser.add_argument(
        "--stance",
        choices=["conservative", "neutral", "aggressive"],
        default="neutral",
        help="Risk stance for recommendations (default: neutral)",
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run, stance=args.stance)
