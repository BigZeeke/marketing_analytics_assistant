"""
app.py
======
Marketing Analytics NL-to-SQL Chat Assistant
- Multi-turn conversation with memory
- Claude converts natural language to Databricks SQL
- Results displayed as table + auto-chart
- Clean recruiter-ready UI
"""

import streamlit as st
import pandas as pd
import anthropic
import sqlite3
import time
import re

# ─── DEPLOYMENT TOGGLE ───────────────────────────────────────────────────────
# Set to True to use Databricks (production), False to use SQLite (demo)
USE_DATABRICKS = False
SQLITE_PATH = "marketing_analytics.db"

# ─── PAGE CONFIG ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Marketing Analytics Assistant",
    page_icon="📊",
    layout="wide",
)

# ─── SCHEMA CONTEXT ───────────────────────────────────────────────────────────
SCHEMA = """
DATABASE: marketing_portfolio.marketing_analytics

CORE TABLES:
- campaigns(campaign_id, campaign_name, channel, campaign_type, objective, budget, spend, status, start_date, end_date, target_audience)
  channel values: Email, Paid, Social, Organic, Display, Referral
  campaign_type values: Nurture, Lead Gen, Awareness, Retargeting
  status values: completed, active, paused, planned, draft

- customers(customer_id, first_name, last_name, email, city, state, segment, points, balance, created_at)
  segment values: Enterprise, SMB, Consumer

- leads(lead_id, campaign_id, customer_id, email, first_name, last_name, lead_source, status, deal_value, score, created_at, converted_at)
  status values: new, qualified, converted, stale
  deal_value: only populated when status = 'converted'

- orders(order_id, customer_id, campaign_id, order_date, shipped_date, status, amount)
- order_items(order_item_id, order_id, product_id, quantity, unit_price)
- products(product_id, name, category, unit_price, quantity_in_stock)
  category values: Software, Services, Support, Training
- payments(payment_id, customer_id, order_id, amount, paid_at)

SEO TABLES:
- seo_keywords(keyword_id, keyword, search_volume, keyword_difficulty, intent_type, topic_cluster)
- seo_rankings(ranking_id, keyword_id, ranking_date, position, page_url, impressions, clicks, ctr_pct)
- organic_traffic(traffic_id, traffic_date, page_url, sessions, new_users, bounce_rate_pct, avg_session_sec, goal_completions)

PPC TABLES:
- ad_groups(ad_group_id, campaign_id, ad_group_name, bid_strategy, max_cpc, status)
- ads(ad_id, ad_group_id, headline_1, headline_2, headline_3, description_1, description_2, final_url, ad_type, status)
- ad_performance(perf_id, ad_id, perf_date, impressions, clicks, spend, conversions, conversion_value, quality_score)

EMAIL TABLES:
- email_campaigns(email_campaign_id, campaign_id, email_name, subject_line, audience_segment, email_type, list_size, send_date, status)
- email_events(event_id, email_campaign_id, customer_id, event_type, event_at)
  event_type values: opened, clicked, converted, unsubscribed

WEB / GTM TABLES:
- gtm_tags(tag_id, tag_name, tag_type, trigger_type, trigger_detail, is_active)
- web_events(web_event_id, session_id, customer_id, tag_id, page_url, event_name, event_category, device_type, traffic_source, created_at)
- web_sessions(session_id, customer_id, landing_page, referrer_source, referrer_medium, utm_campaign, device_type, session_start, session_end, pages_viewed, converted)

CONTENT TABLES:
- content_pieces(content_id, title, content_type, topic_cluster, target_keyword, author, word_count, publish_date, status, cta_type, campaign_id)
- content_performance(perf_id, content_id, perf_date, page_views, unique_visitors, avg_time_sec, bounce_rate_pct, social_shares, comments, backlinks_earned, cta_clicks, conversions)

AUDIENCE / TEST TABLES:
- audiences(audience_id, audience_name, channel, audience_type, criteria_description, size_estimate, match_rate_pct, is_active)
- audience_members(member_id, audience_id, customer_id, added_at)
- ab_tests(test_id, test_name, test_type, campaign_id, content_id, email_campaign_id, hypothesis, start_date, end_date, status, winner_variant, confidence_pct, primary_metric)
  test_type values: subject_line, ad_copy, landing_page, cta, audience, bid_strategy
  status values: running, completed, stopped
  winner_variant values: A, B (which variant won)
- ab_variants(variant_id, test_id, variant_name, variant_detail, impressions, conversions, revenue)
  variant_name values: control, variant_a (always exactly these two values per test)
  To calculate conversion lift: compare conversions/impressions between variant_a and control

IMPORTANT RULES:
- Always prefix tables: marketing_portfolio.marketing_analytics.<table>
- ROAS = SUM(deal_value) / SUM(spend) using only leads WHERE status = 'converted' AND deal_value IS NOT NULL
- dates are stored as TEXT in ISO format (YYYY-MM-DD)
"""

_SQLITE_RULES = """
SQL DIALECT: SQLite
- Date filtering: strftime('%Y', date_col) = '2024' — never use YEAR()
- Month grouping: strftime('%Y-%m', date_col) — never use DATE_TRUNC
- No QUALIFY clause — use subqueries instead
- No PIVOT — use CASE WHEN for pivoting
- Boolean values stored as 1/0 integers
- No semicolons at end of queries
- Table names do NOT need catalog prefix — use table name only (e.g. campaigns not marketing_portfolio.marketing_analytics.campaigns)
"""

_DATABRICKS_RULES = """
SQL DIALECT: Databricks SQL (Delta Lake)
- Date filtering: YEAR(date_col) = 2024 or date_col >= '2024-01-01'
- Month grouping: DATE_TRUNC('month', date_col)
- QUALIFY clause supported for window function filtering
- Boolean values are TRUE/FALSE
- No semicolons at end of queries
- Always use fully qualified table names: marketing_portfolio.marketing_analytics.<table_name>
"""


def build_system_prompt():
    dialect_rules = _DATABRICKS_RULES if USE_DATABRICKS else _SQLITE_RULES
    dialect_name = "Databricks SQL" if USE_DATABRICKS else "SQLite"
    return f"""You are a marketing analytics SQL assistant. You help users explore marketing data by writing precise {dialect_name} queries.

When the user asks a question:
1. Write a correct {dialect_name} query that answers it
2. Wrap the SQL in ```sql code blocks
3. After the SQL, briefly explain what the query does in 1-2 sentences
4. If the user asks a follow-up, use context from the conversation to refine or extend the previous query

Never use semicolons at the end of queries.
Return only one SQL query per response.

{dialect_rules}

DATABASE SCHEMA:
{SCHEMA}
"""


SYSTEM_PROMPT = build_system_prompt()

# ─── HELPERS ──────────────────────────────────────────────────────────────────


def get_anthropic_client():
    return anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])


def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL against Databricks or SQLite depending on USE_DATABRICKS flag."""
    if USE_DATABRICKS:
        from databricks import sql as databricks_sql
        cfg = st.secrets["databricks"]
        conn = databricks_sql.connect(
            server_hostname=cfg["server_hostname"],
            http_path=cfg["http_path"],
            access_token=cfg["access_token"],
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                cols = [d[0] for d in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=cols)
        finally:
            conn.close()
    else:
        # SQLite mode — strip catalog/schema prefix if present
        clean_sql = re.sub(
            r"marketing_portfolio\.marketing_analytics\.", "", sql)
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            df = pd.read_sql_query(clean_sql, conn)
            return df
        finally:
            conn.close()


def extract_sql(text: str) -> str | None:
    """Pull the first ```sql ... ``` block from Claude's response."""
    match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*(SELECT|WITH).*?```",
                      text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0).replace("```", "").strip()
    return None


def ask_claude(messages: list) -> str:
    """Send conversation history to Claude and get a response."""
    client = get_anthropic_client()
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        system=build_system_prompt(),
        messages=messages,
    )
    return response.content[0].text


def render_chart(df: pd.DataFrame):
    """Auto-render the best chart for the data."""
    if df.empty or len(df.columns) < 2:
        return

    # Find numeric columns
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

    if not numeric_cols:
        return

    # If there's a good categorical/date index, use it
    if non_numeric_cols:
        try:
            chart_df = df.set_index(non_numeric_cols[0])[numeric_cols]
            if len(numeric_cols) == 1:
                st.bar_chart(chart_df)
            else:
                st.line_chart(chart_df)
            return
        except Exception:
            pass

    # Fallback: just plot numeric columns
    st.line_chart(df[numeric_cols])


# ─── SESSION STATE ────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []  # Claude conversation history
if "chat_display" not in st.session_state:
    st.session_state.chat_display = []  # UI display items
if "question_count" not in st.session_state:
    st.session_state.question_count = 0  # Rate limit counter

MAX_QUESTIONS = 20  # Per session limit

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Marketing Analytics")
    st.markdown("Ask questions about your data in plain English.")
    questions_used = st.session_state.get("question_count", 0)
    questions_left = MAX_QUESTIONS - questions_used
    st.caption(
        f"Questions remaining this session: {questions_left}/{MAX_QUESTIONS}")
    st.markdown("---")

    with st.expander("ℹ️ How to use this app", expanded=False):
        st.markdown("""
**Analytics Assistant tab**
- Type any question about the marketing data in plain English
- Claude translates it to SQL, runs it, and returns results with a chart
- Use the question bank below to explore by role, or write your own

**Recommendations tab**
- Select a risk stance (Conservative / Neutral / Aggressive) to tune how Claude evaluates performance thresholds
- Click **Refresh Recommendations** to re-run the monitor and generate a fresh set
- Review each card and mark it as **Applied**, **Dismissed**, or leave it **Pending**
- Conservative = smaller suggested changes, higher confidence bar
- Aggressive = lower tolerance for underperformance, bolder reallocation suggestions
        """)

    st.markdown("---")
    st.caption(
        "Select a role to load relevant sample questions, or type your own in the chat.")
    persona = st.selectbox("Question bank", [
                           "👔 CMO", "📣 Digital Marketing Manager", "🔧 Analytics Engineer"])

    st.markdown("---")

    if persona == "👔 CMO":
        sections = {
            "Revenue & Pipeline": [
                "Total revenue by customer segment in 2024",
                "Monthly revenue trend across all of 2024",
                "Which campaigns generated the most revenue?",
                "Average deal value by channel",
                "Top 10 customers by lifetime order value",
            ],
            "Campaign ROI": [
                "ROAS by channel using converted leads only",
                "Budget vs actual spend by campaign",
                "Cost per lead by channel",
                "Which campaigns had the best conversion rate?",
                "Lead volume by quarter in 2024",
            ],
            "Funnel Health": [
                "Lead status breakdown across all campaigns",
                "How long does it take leads to convert on average?",
                "Which channels produce the highest quality leads by score?",
                "Conversion rate by lead source",
                "Which campaigns have the most stale leads?",
            ],
        }

    elif persona == "📣 Digital Marketing Manager":
        sections = {
            "Email": [
                "Open and click rates by email campaign",
                "Which email type has the highest conversion rate?",
                "Unsubscribe rate by audience segment",
                "Which email campaigns drove the most conversions?",
                "Email list size by campaign",
            ],
            "SEO & Content": [
                "Keyword rankings that improved from 2023 to 2024",
                "Organic traffic sessions by page over time",
                "Top pages by goal completions",
                "Content conversion rate by content type",
                "Which content pieces have the most backlinks?",
            ],
            "Paid & PPC": [
                "Ad performance: impressions, clicks, spend by ad group",
                "Click-through rate trend by ad",
                "Which ads have the highest quality score?",
                "Cost per conversion by ad group",
                "Which ad groups are paused vs active?",
            ],
            "Web & A/B Tests": [
                "Top landing pages by session count",
                "Conversion rate by traffic source",
                "Sessions by device type",
                "A/B test results: variant_a vs control conversion rate",
                "Which A/B tests had the highest revenue lift?",
            ],
        }

    else:
        sections = {
            "Data Quality": [
                "Are there any leads with a campaign_id that does not exist in campaigns?",
                "Which orders have no matching payment record?",
                "How many leads have a null customer_id vs linked customer?",
                "Are there duplicate emails in the leads table?",
                "Which email events have no matching email campaign?",
            ],
            "Funnel Metrics": [
                "Lead-to-order conversion rate by campaign with null handling",
                "Rolling 30-day lead volume by channel",
                "Cumulative revenue by month using a window function",
                "Rank campaigns by ROAS using DENSE_RANK",
                "Month-over-month lead growth rate by channel",
            ],
            "Attribution": [
                "First touch attribution: revenue by first lead source",
                "Which campaigns appear in both leads and orders?",
                "Customer journey: leads who became customers with orders",
                "Average days from lead created to order placed",
                "Multi-channel customers: how many segments per customer?",
            ],
            "Performance": [
                "Average CTR by ad group ranked by spend",
                "Email funnel: sent to open to click to convert rates",
                "SEO rank position change from first to last snapshot per keyword",
                "Content pieces with above-average conversion rates",
                "A/B test statistical summary: lift and confidence by test type",
            ],
        }

    for section, questions in sections.items():
        st.markdown(f"**{section}**")
        for ex in questions:
            if st.button(ex, key=ex, use_container_width=True):
                st.session_state._prefill = ex

    st.markdown("---")
    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.chat_display = []
        st.rerun()

# ─── MAIN UI ──────────────────────────────────────────────────────────────────
st.title("📊 Marketing Analytics Assistant")
st.caption("Ask questions about campaigns, leads, revenue, SEO, email, and more.")

tab_chat, tab_recs = st.tabs(["💬 Analytics Assistant", "💡 Recommendations"])

# ─── TAB 2: RECOMMENDATIONS ───────────────────────────────────────────────────
with tab_recs:
    st.markdown("### 💡 AI-Generated Marketing Recommendations")
    st.info(
        "**Note:** These recommendations are generated by Claude based on the metrics "
        "in this dataset. Because this app uses synthetic test data, some findings may "
        "appear extreme (e.g. very high CPAs or near-zero ROAS). The intent is to "
        "demonstrate the pattern: query the data → send to Claude → surface prioritized, "
        "data-backed recommendations for human review.",
        icon="ℹ️",
    )

    # ── Helpers ───────────────────────────────────────────────────────────────
    def migrate_schema():
        """Add notes and actioned_at columns if they don't exist yet."""
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            existing = [r[1] for r in conn.execute(
                "PRAGMA table_info(recommendations)").fetchall()]
            if "notes" not in existing:
                conn.execute(
                    "ALTER TABLE recommendations ADD COLUMN notes TEXT")
            if "actioned_at" not in existing:
                conn.execute(
                    "ALTER TABLE recommendations ADD COLUMN actioned_at TEXT")
            conn.commit()
        except Exception:
            pass
        finally:
            conn.close()

    migrate_schema()

    def load_recommendations() -> pd.DataFrame:
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            df = pd.read_sql_query("""
                SELECT
                    rec_id,
                    run_at,
                    priority,
                    channel,
                    metric,
                    current_value,
                    action,
                    rationale,
                    simulated_impact,
                    ROUND(confidence * 100) || '%' AS confidence,
                    status,
                    COALESCE(notes, '')       AS notes,
                    COALESCE(actioned_at, '') AS actioned_at
                FROM recommendations
                ORDER BY
                    CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                    rec_id
            """, conn)
            return df
        except Exception:
            return pd.DataFrame()
        finally:
            conn.close()

    def update_status(rec_id: int, new_status: str, notes: str = ""):
        from datetime import datetime, timezone
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            actioned_at = datetime.now(timezone.utc).isoformat(
                timespec="seconds") if new_status != "pending" else None
            conn.execute(
                "UPDATE recommendations SET status = ?, notes = ?, actioned_at = ? WHERE rec_id = ?",
                (new_status, notes.strip() or None, actioned_at, rec_id)
            )
            conn.commit()
        finally:
            conn.close()

    def escape_dollars(text: str) -> str:
        return text.replace("$", r"\$") if text else text

    def run_monitor(stance: str):
        import subprocess
        import sys
        result = subprocess.run(
            [sys.executable, "monitor.py", "--stance", stance],
            capture_output=True, text=True
        )
        return result.returncode == 0, result.stdout, result.stderr

    # ── Stance selector + re-run controls ─────────────────────────────────────
    st.markdown("#### ⚙️ Monitor Controls")

    stance_descriptions = {
        "Conservative": "Lower sensitivity — only flags severe underperformance. Prefers monitoring over action. Suggests smaller budget changes (≤15%).",
        "Neutral":      "Balanced — flags channels below standard ROAS and CPA thresholds. Recommends moderate changes (≤30%).",
        "Aggressive":   "Higher sensitivity — flags any underperformance quickly. Prefers immediate action. Suggests larger reallocations (up to 50%).",
    }

    ctrl_col1, ctrl_col2 = st.columns([2, 1])
    with ctrl_col1:
        stance = st.selectbox(
            "Risk stance",
            ["Conservative", "Neutral", "Aggressive"],
            index=1,
            help="Controls how Claude evaluates performance thresholds and sizes its recommendations.",
        )
        st.caption(stance_descriptions[stance])
    with ctrl_col2:
        st.markdown("&nbsp;", unsafe_allow_html=True)
        run_clicked = st.button(
            "🔄 Refresh Recommendations", use_container_width=True)

    if run_clicked:
        with st.spinner(f"Running monitor in **{stance}** mode — analyzing your marketing data..."):
            success, stdout, stderr = run_monitor(stance.lower())
        if success:
            st.success(
                f"✅ Monitor complete. Recommendations refreshed in **{stance}** mode.")
            st.rerun()
        else:
            st.error("Monitor run failed. See details below.")
            st.code(stderr or stdout)

    st.markdown("---")

    # ── Load & display ─────────────────────────────────────────────────────────
    recs_df = load_recommendations()

    if recs_df.empty:
        st.warning(
            "No recommendations found. Click **Refresh Recommendations** above to generate them.")
    else:
        # Summary metrics
        total = len(recs_df)
        high_count = len(recs_df[recs_df["priority"] == "high"])
        med_count = len(recs_df[recs_df["priority"] == "medium"])
        low_count = len(recs_df[recs_df["priority"] == "low"])
        pending_count = len(recs_df[recs_df["status"] == "pending"])
        applied_count = len(recs_df[recs_df["status"] == "applied"])
        last_run = recs_df["run_at"].iloc[0] if "run_at" in recs_df.columns else "—"

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total",           total)
        m2.metric("🔴 High",         high_count)
        m3.metric("🟡 Medium",       med_count)
        m4.metric("⏳ Pending",      pending_count)
        m5.metric("✅ Applied",      applied_count)
        st.caption(f"Last monitor run: {last_run}")

        st.markdown("---")

        # Filters row
        f1, f2 = st.columns(2)
        with f1:
            priority_filter = st.radio(
                "Filter by priority",
                ["All", "🔴 High", "🟡 Medium", "🟢 Low"],
                horizontal=True,
            )
        with f2:
            status_filter = st.radio(
                "Filter by status",
                ["All", "⏳ Pending", "✅ Applied", "❌ Dismissed"],
                horizontal=True,
            )

        priority_map = {"All": None, "🔴 High": "high",
                        "🟡 Medium": "medium", "🟢 Low": "low"}
        status_map = {"All": None, "⏳ Pending": "pending",
                      "✅ Applied": "applied", "❌ Dismissed": "dismissed"}

        filtered_df = recs_df.copy()
        if priority_map[priority_filter]:
            filtered_df = filtered_df[filtered_df["priority"]
                                      == priority_map[priority_filter]]
        if status_map[status_filter]:
            filtered_df = filtered_df[filtered_df["status"]
                                      == status_map[status_filter]]

        st.markdown("---")
        st.caption(f"Showing {len(filtered_df)} of {total} recommendations")

        # ── Recommendation cards ───────────────────────────────────────────────
        priority_icons = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        status_icons = {"pending": "⏳", "applied": "✅", "dismissed": "❌"}

        for _, row in filtered_df.iterrows():
            p_icon = priority_icons.get(row["priority"], "⚪")
            s_icon = status_icons.get(row["status"], "⏳")

            with st.container(border=True):
                top_left, top_right = st.columns([5, 1])

                with top_left:
                    st.markdown(f"**{p_icon} {row['action']}**")
                    st.caption(
                        f"Channel: **{row['channel'] or 'Cross-channel'}** · "
                        f"Metric: **{row['metric'] or '—'}** · "
                        f"Value: {row['current_value'] or '—'}"
                    )
                with top_right:
                    st.markdown(f"**{row['confidence']}** confidence")
                    st.caption(f"{p_icon} {row['priority'].capitalize()}")

                st.markdown(
                    f"**Rationale:** {escape_dollars(row['rationale'])}")
                if row.get("simulated_impact"):
                    st.markdown(
                        f"**Estimated impact:** {escape_dollars(row['simulated_impact'])}")

                st.markdown("")
                status_col, notes_col = st.columns([2, 4])

                with status_col:
                    status_options = ["pending", "applied", "dismissed"]
                    current_index = status_options.index(
                        row["status"]) if row["status"] in status_options else 0
                    new_status = st.selectbox(
                        "Status",
                        options=status_options,
                        index=current_index,
                        key=f"status_{row['rec_id']}",
                        format_func=lambda s: {
                            "pending": "⏳ Pending", "applied": "✅ Applied", "dismissed": "❌ Dismissed"}[s],
                        label_visibility="collapsed",
                    )

                with notes_col:
                    if new_status == "applied":
                        notes_input = st.text_input(
                            "What did you do?",
                            value=row.get("notes", ""),
                            placeholder="e.g. Reduced Display budget 20%, moved $47K to Referral",
                            key=f"notes_{row['rec_id']}",
                        )
                    elif new_status == "dismissed":
                        dismiss_reasons = [
                            "Select a reason…",
                            "Not enough data",
                            "Budget constraints",
                            "Already in progress",
                            "Disagree with recommendation",
                            "Deferred to next cycle",
                        ]
                        current_note = row.get(
                            "notes", "") or "Select a reason…"
                        reason_index = dismiss_reasons.index(
                            current_note) if current_note in dismiss_reasons else 0
                        notes_input = st.selectbox(
                            "Reason for dismissing",
                            options=dismiss_reasons,
                            index=reason_index,
                            key=f"notes_{row['rec_id']}",
                            label_visibility="collapsed",
                        )
                        if notes_input == "Select a reason…":
                            notes_input = ""
                    else:
                        notes_input = row.get("notes", "")

                # Show actioned timestamp if already actioned
                if row.get("actioned_at"):
                    st.caption(f"Actioned: {row['actioned_at']}")

                # Write back on any change
                status_changed = new_status != row["status"]
                notes_changed = notes_input.strip() != row.get("notes", "").strip()
                if (status_changed or notes_changed) and new_status != "pending":
                    update_status(int(row["rec_id"]), new_status, notes_input)
                    st.rerun()
                elif status_changed and new_status == "pending":
                    update_status(int(row["rec_id"]), "pending", "")
                    st.rerun()

        st.markdown("---")

        # ── Action log ────────────────────────────────────────────────────────
        actioned_df = recs_df[recs_df["status"].isin(
            ["applied", "dismissed"])].copy()
        if not actioned_df.empty:
            with st.expander(f"📋 Action log ({len(actioned_df)} items)", expanded=False):
                log_df = actioned_df[[
                    "actioned_at", "channel", "action", "status", "notes", "simulated_impact"
                ]].rename(columns={
                    "actioned_at":      "Actioned at",
                    "channel":          "Channel",
                    "action":           "Recommendation",
                    "status":           "Status",
                    "notes":            "Notes / Reason",
                    "simulated_impact": "Estimated impact",
                })
                log_df["Status"] = log_df["Status"].map(
                    {"applied": "✅ Applied", "dismissed": "❌ Dismissed"}
                )
                st.dataframe(log_df, use_container_width=True, hide_index=True)

        with st.expander("View raw recommendations table"):
            st.dataframe(
                filtered_df.drop(
                    columns=["rec_id", "run_at"], errors="ignore"),
                use_container_width=True,
            )

# ─── TAB 1: CHAT ──────────────────────────────────────────────────────────────
with tab_chat:
    # Render chat history
    for item in st.session_state.chat_display:
        if item["role"] == "user":
            with st.chat_message("user"):
                st.markdown(item["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(item["content"])
                if item.get("sql"):
                    with st.expander("View SQL", expanded=False):
                        st.code(item["sql"], language="sql")
                if item.get("dataframe") is not None:
                    df = item["dataframe"]
                    st.dataframe(df, use_container_width=True)
                    render_chart(df)
                if item.get("error"):
                    st.error(item["error"])

    # ─── INPUT ────────────────────────────────────────────────────────────────
    # Handle sidebar button prefill
    prefill = st.session_state.pop("_prefill", None)

    user_input = st.chat_input("Ask a question about your marketing data...")

    # Use prefill if sidebar button was clicked
    if prefill and not user_input:
        user_input = prefill

    if user_input:
        # Rate limit check
        if st.session_state.question_count >= MAX_QUESTIONS:
            st.warning(
                f"Session limit of {MAX_QUESTIONS} questions reached. Please refresh the page to start a new session.")
            st.stop()

        # Show user message
        with st.chat_message("user"):
            st.markdown(user_input)
        st.session_state.chat_display.append(
            {"role": "user", "content": user_input})
        st.session_state.question_count += 1

        # Add to Claude conversation history
        st.session_state.messages.append(
            {"role": "user", "content": user_input})

        # Get Claude response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                start = time.time()
                reply = ask_claude(st.session_state.messages)
                elapsed = time.time() - start

            # Add Claude reply to conversation history
            st.session_state.messages.append(
                {"role": "assistant", "content": reply})

            # Extract SQL
            sql_query = extract_sql(reply)

            # Strip SQL block from display text for cleaner rendering
            display_text = re.sub(r"```sql.*?```", "",
                                  reply, flags=re.DOTALL).strip()
            st.markdown(display_text)

            display_item = {
                "role": "assistant",
                "content": display_text,
                "sql": sql_query,
                "dataframe": None,
                "error": None,
            }

            if sql_query:
                with st.expander("View SQL", expanded=True):
                    st.code(sql_query, language="sql")

                # Run the query
                with st.spinner("Running query..."):
                    try:
                        df = run_query(sql_query)
                        display_item["dataframe"] = df
                        st.dataframe(df, use_container_width=True)
                        render_chart(df)
                        st.caption(
                            f"Query returned {len(df)} rows in {elapsed:.1f}s")
                    except Exception as e:
                        err = f"Query error: {str(e)}"
                        display_item["error"] = err
                        st.error(err)

            st.session_state.chat_display.append(display_item)
