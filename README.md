# Marketing Analytics Assistant

A natural language to SQL chat app built with Streamlit, Claude, and Databricks. Ask questions about marketing data in plain English and get instant SQL queries, results, and visualizations.

## Demo

Ask questions like:
- *"ROAS by channel using converted leads only"*
- *"Monthly revenue trend across all of 2024"*
- *"Email funnel: open to click to convert rates"*
- *"Rank campaigns by ROAS using DENSE_RANK"*

The app writes the SQL, runs it against a live Databricks warehouse, and returns results with auto-generated charts.

## Features

- **Multi-turn conversation** — ask follow-up questions with full context memory
- **Three persona question banks** — CMO, Digital Marketing Manager, Analytics Engineer
- **Auto-visualization** — bar and line charts generated from query results
- **Live SQL execution** — queries run directly against Databricks Serverless SQL
- **23-table marketing schema** — campaigns, leads, orders, email, SEO, PPC, content, A/B tests, web events

## Tech Stack

- [Streamlit](https://streamlit.io) — UI framework
- [Anthropic Claude](https://anthropic.com) — NL-to-SQL via claude-opus-4-5
- [Databricks SQL Connector](https://docs.databricks.com/dev-tools/python-sql-connector.html) — query execution
- [Pandas](https://pandas.pydata.org) — data handling

## Setup

1. Clone the repo
2. Create a virtual environment and install dependencies:
```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
```
3. Create `.streamlit/secrets.toml`:
```toml
   [databricks]
   server_hostname = "your-workspace.azuredatabricks.net"
   http_path       = "/sql/1.0/warehouses/your-warehouse-id"
   access_token    = "your-databricks-token"
   catalog         = "your-catalog"
   schema          = "your-schema"

   [anthropic]
   api_key = "your-anthropic-api-key"
```
4. Run the app:
```bash
   streamlit run app.py
```

## Data Model

The app queries a 23-table marketing analytics schema covering:

| Domain | Tables |
|--------|--------|
| Core | campaigns, customers, leads, orders, products, payments |
| Email | email_campaigns, email_events |
| SEO | seo_keywords, seo_rankings, organic_traffic |
| PPC | ad_groups, ads, ad_performance |
| Web | web_sessions, web_events, gtm_tags |
| Content | content_pieces, content_performance |
| Audiences | audiences, audience_members |
| Testing | ab_tests, ab_variants |

## Author

Steve Lopez — Data Analyst at VCA Animal Hospitals  
Building toward Analytics Engineering on Azure/Databricks
