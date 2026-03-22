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
from databricks import sql as databricks_sql
import time
import re

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
- Use Databricks SQL syntax (no semicolons inside CTEs, QUALIFY instead of subqueries where possible)
- dates are DATE type, timestamps are TIMESTAMP type
"""

SYSTEM_PROMPT = f"""You are a marketing analytics SQL assistant. You help users explore marketing data by writing precise Databricks SQL queries.

When the user asks a question:
1. Write a clean Databricks SQL query that answers it
2. Wrap the SQL in ```sql code blocks
3. After the SQL, briefly explain what the query does in 1-2 sentences
4. If the user asks a follow-up, use context from the conversation to refine or extend the previous query

Always use fully qualified table names: marketing_portfolio.marketing_analytics.<table_name>
Never use semicolons at the end of queries.
Return only one SQL query per response.

DATABASE SCHEMA:
{SCHEMA}
"""

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def get_anthropic_client():
    return anthropic.Anthropic(api_key=st.secrets["anthropic"]["api_key"])

def get_db_connection():
    cfg = st.secrets["databricks"]
    return databricks_sql.connect(
        server_hostname=cfg["server_hostname"],
        http_path=cfg["http_path"],
        access_token=cfg["access_token"],
    )

def extract_sql(text: str) -> str | None:
    """Pull the first ```sql ... ``` block from Claude's response."""
    match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*(SELECT|WITH).*?```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0).replace("```", "").strip()
    return None

def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            cols = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

def ask_claude(messages: list) -> str:
    """Send conversation history to Claude and get a response."""
    client = get_anthropic_client()
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
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

# ─── SIDEBAR ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Marketing Analytics")
    st.markdown("Ask questions about your data in plain English.")
    st.markdown("---")
    persona = st.selectbox("Question bank", ["👔 CMO", "📣 Digital Marketing Manager", "🔧 Analytics Engineer"])

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

# ─── INPUT ────────────────────────────────────────────────────────────────────
# Handle sidebar button prefill
prefill = st.session_state.pop("_prefill", None)

user_input = st.chat_input("Ask a question about your marketing data...")

# Use prefill if sidebar button was clicked
if prefill and not user_input:
    user_input = prefill

if user_input:
    # Show user message
    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.chat_display.append({"role": "user", "content": user_input})

    # Add to Claude conversation history
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Get Claude response
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            start = time.time()
            reply = ask_claude(st.session_state.messages)
            elapsed = time.time() - start

        # Add Claude reply to conversation history
        st.session_state.messages.append({"role": "assistant", "content": reply})

        # Extract SQL
        sql_query = extract_sql(reply)

        # Strip SQL block from display text for cleaner rendering
        display_text = re.sub(r"```sql.*?```", "", reply, flags=re.DOTALL).strip()
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
                    st.caption(f"Query returned {len(df)} rows in {elapsed:.1f}s")
                except Exception as e:
                    err = f"Query error: {str(e)}"
                    display_item["error"] = err
                    st.error(err)

        st.session_state.chat_display.append(display_item)
