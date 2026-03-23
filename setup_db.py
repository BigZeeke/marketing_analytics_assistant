"""
setup_db.py
===========
Creates and populates a SQLite database with all 23 marketing analytics tables.
Run once locally, then commit the generated marketing_analytics.db file to the repo.

Usage:
    python3 setup_db.py
"""

import sqlite3
import os

DB_PATH = "marketing_analytics.db"

def create_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")

    print("Creating tables...")

    c.executescript("""
    CREATE TABLE campaigns (
        campaign_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_name   TEXT NOT NULL,
        channel         TEXT NOT NULL,
        campaign_type   TEXT NOT NULL,
        objective       TEXT,
        budget          REAL,
        spend           REAL,
        status          TEXT,
        start_date      TEXT,
        end_date        TEXT,
        target_audience TEXT
    );

    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name  TEXT,
        last_name   TEXT,
        email       TEXT,
        city        TEXT,
        state       TEXT,
        segment     TEXT,
        points      INTEGER,
        balance     REAL,
        created_at  TEXT
    );

    CREATE TABLE products (
        product_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name                TEXT NOT NULL,
        category            TEXT NOT NULL,
        unit_price          REAL NOT NULL,
        quantity_in_stock   INTEGER
    );

    CREATE TABLE leads (
        lead_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id  INTEGER REFERENCES campaigns(campaign_id),
        customer_id  INTEGER REFERENCES customers(customer_id),
        email        TEXT,
        first_name   TEXT,
        last_name    TEXT,
        lead_source  TEXT,
        status       TEXT,
        deal_value   REAL,
        score        INTEGER,
        created_at   TEXT,
        converted_at TEXT
    );

    CREATE TABLE orders (
        order_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id  INTEGER REFERENCES customers(customer_id),
        campaign_id  INTEGER REFERENCES campaigns(campaign_id),
        order_date   TEXT,
        shipped_date TEXT,
        status       TEXT,
        amount       REAL
    );

    CREATE TABLE order_items (
        order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id      INTEGER REFERENCES orders(order_id),
        product_id    INTEGER REFERENCES products(product_id),
        quantity      INTEGER,
        unit_price    REAL
    );

    CREATE TABLE payments (
        payment_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_id    INTEGER REFERENCES orders(order_id),
        amount      REAL,
        paid_at     TEXT
    );

    CREATE TABLE seo_keywords (
        keyword_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword             TEXT,
        search_volume       INTEGER,
        keyword_difficulty  INTEGER,
        intent_type         TEXT,
        topic_cluster       TEXT
    );

    CREATE TABLE seo_rankings (
        ranking_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword_id   INTEGER REFERENCES seo_keywords(keyword_id),
        ranking_date TEXT,
        position     INTEGER,
        page_url     TEXT,
        impressions  INTEGER,
        clicks       INTEGER,
        ctr_pct      REAL
    );

    CREATE TABLE organic_traffic (
        traffic_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        traffic_date     TEXT,
        page_url         TEXT,
        sessions         INTEGER,
        new_users        INTEGER,
        bounce_rate_pct  REAL,
        avg_session_sec  INTEGER,
        goal_completions INTEGER
    );

    CREATE TABLE gtm_tags (
        tag_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        tag_name        TEXT,
        tag_type        TEXT,
        trigger_type    TEXT,
        trigger_detail  TEXT,
        is_active       INTEGER
    );

    CREATE TABLE ad_groups (
        ad_group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id   INTEGER REFERENCES campaigns(campaign_id),
        ad_group_name TEXT,
        bid_strategy  TEXT,
        max_cpc       REAL,
        status        TEXT
    );

    CREATE TABLE ads (
        ad_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        ad_group_id   INTEGER REFERENCES ad_groups(ad_group_id),
        headline_1    TEXT,
        headline_2    TEXT,
        headline_3    TEXT,
        description_1 TEXT,
        description_2 TEXT,
        final_url     TEXT,
        ad_type       TEXT,
        status        TEXT
    );

    CREATE TABLE ad_performance (
        perf_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ad_id            INTEGER REFERENCES ads(ad_id),
        perf_date        TEXT,
        impressions      INTEGER,
        clicks           INTEGER,
        spend            REAL,
        conversions      INTEGER,
        conversion_value REAL,
        quality_score    INTEGER
    );

    CREATE TABLE email_campaigns (
        email_campaign_id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id       INTEGER REFERENCES campaigns(campaign_id),
        email_name        TEXT,
        subject_line      TEXT,
        preview_text      TEXT,
        sender_name       TEXT,
        audience_segment  TEXT,
        email_type        TEXT,
        list_size         INTEGER,
        send_date         TEXT,
        status            TEXT
    );

    CREATE TABLE email_events (
        event_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        email_campaign_id INTEGER REFERENCES email_campaigns(email_campaign_id),
        customer_id       INTEGER REFERENCES customers(customer_id),
        event_type        TEXT,
        event_at          TEXT
    );

    CREATE TABLE web_events (
        web_event_id  INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id    TEXT,
        customer_id   INTEGER REFERENCES customers(customer_id),
        tag_id        INTEGER REFERENCES gtm_tags(tag_id),
        page_url      TEXT,
        event_name    TEXT,
        event_category TEXT,
        device_type   TEXT,
        traffic_source TEXT,
        created_at    TEXT
    );

    CREATE TABLE web_sessions (
        session_id      TEXT PRIMARY KEY,
        customer_id     INTEGER REFERENCES customers(customer_id),
        landing_page    TEXT,
        referrer_source TEXT,
        referrer_medium TEXT,
        utm_campaign    TEXT,
        device_type     TEXT,
        session_start   TEXT,
        session_end     TEXT,
        pages_viewed    INTEGER,
        converted       INTEGER
    );

    CREATE TABLE content_pieces (
        content_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        title          TEXT,
        content_type   TEXT,
        topic_cluster  TEXT,
        target_keyword TEXT,
        author         TEXT,
        word_count     INTEGER,
        publish_date   TEXT,
        status         TEXT,
        cta_type       TEXT,
        campaign_id    INTEGER REFERENCES campaigns(campaign_id)
    );

    CREATE TABLE content_performance (
        perf_id          INTEGER PRIMARY KEY AUTOINCREMENT,
        content_id       INTEGER REFERENCES content_pieces(content_id),
        perf_date        TEXT,
        page_views       INTEGER,
        unique_visitors  INTEGER,
        avg_time_sec     INTEGER,
        bounce_rate_pct  REAL,
        social_shares    INTEGER,
        comments         INTEGER,
        backlinks_earned INTEGER,
        cta_clicks       INTEGER,
        conversions      INTEGER
    );

    CREATE TABLE audiences (
        audience_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        audience_name         TEXT,
        channel               TEXT,
        audience_type         TEXT,
        criteria_description  TEXT,
        size_estimate         INTEGER,
        match_rate_pct        REAL,
        is_active             INTEGER
    );

    CREATE TABLE audience_members (
        member_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        audience_id INTEGER REFERENCES audiences(audience_id),
        customer_id INTEGER REFERENCES customers(customer_id),
        added_at    TEXT
    );

    CREATE TABLE ab_tests (
        test_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        test_name         TEXT,
        test_type         TEXT,
        campaign_id       INTEGER REFERENCES campaigns(campaign_id),
        content_id        INTEGER REFERENCES content_pieces(content_id),
        email_campaign_id INTEGER REFERENCES email_campaigns(email_campaign_id),
        hypothesis        TEXT,
        start_date        TEXT,
        end_date          TEXT,
        status            TEXT,
        winner_variant    TEXT,
        confidence_pct    REAL,
        primary_metric    TEXT
    );

    CREATE TABLE ab_variants (
        variant_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        test_id       INTEGER REFERENCES ab_tests(test_id),
        variant_name  TEXT,
        variant_detail TEXT,
        impressions   INTEGER,
        conversions   INTEGER,
        revenue       REAL
    );
    """)

    print("Inserting data...")

    # ── PRODUCTS ──────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO products (name, category, unit_price, quantity_in_stock) VALUES (?,?,?,?)", [
        ('Marketing Automation Platform - Starter',     'Software',  299.00,  500),
        ('Marketing Automation Platform - Pro',          'Software',  799.00,  500),
        ('Marketing Automation Platform - Enterprise',   'Software', 1999.00,  200),
        ('CRM Integration Module',                       'Software',  399.00,  300),
        ('Email Analytics Add-on',                       'Software',  149.00,  400),
        ('SEO Toolkit Annual License',                   'Software',  599.00,  300),
        ('Social Media Scheduler - Pro',                 'Software',  249.00,  500),
        ('Attribution Reporting Module',                 'Software',  499.00,  250),
        ('Onboarding Consultation - 4hr',                'Services',  800.00,  999),
        ('Strategy Workshop - Full Day',                 'Services', 2400.00,  999),
        ('Campaign Audit Service',                       'Services', 1200.00,  999),
        ('Data Migration Service',                       'Services', 3500.00,  999),
        ('Implementation Package - Basic',               'Services', 1800.00,  999),
        ('Implementation Package - Enterprise',          'Services', 6000.00,  999),
        ('Annual Support Contract - Standard',           'Support',  1200.00,  999),
        ('Annual Support Contract - Premium',            'Support',  2400.00,  999),
        ('Training Course - Analytics Fundamentals',     'Training',  299.00,  999),
        ('Training Course - Advanced SQL for Marketers', 'Training',  499.00,  999),
        ('Training Course - dbt and Data Modeling',      'Training',  699.00,  999),
        ('Certification Exam Voucher',                   'Training',  199.00,  999),
    ])

    # ── CUSTOMERS ─────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO customers (first_name,last_name,email,city,state,segment,points,balance,created_at) VALUES (?,?,?,?,?,?,?,?,?)", [
        ('Alice','Johnson','alice.johnson@enterprise.com','New York','NY','Enterprise',8500,125000.00,'2022-01-15'),
        ('Bob','Smith','bob.smith@enterprise.com','Los Angeles','CA','Enterprise',7200,98000.00,'2022-02-10'),
        ('Carol','Williams','carol.williams@enterprise.com','Chicago','IL','Enterprise',9100,145000.00,'2022-01-20'),
        ('David','Brown','david.brown@smb.com','Houston','TX','SMB',3400,28000.00,'2022-03-05'),
        ('Emma','Davis','emma.davis@enterprise.com','Phoenix','AZ','Enterprise',8800,132000.00,'2022-02-18'),
        ('Frank','Miller','frank.miller@consumer.com','Philadelphia','PA','Consumer',1200,4500.00,'2022-04-01'),
        ('Grace','Wilson','grace.wilson@enterprise.com','San Antonio','TX','Enterprise',7600,115000.00,'2022-03-12'),
        ('Henry','Moore','henry.moore@consumer.com','San Diego','CA','Consumer',900,2800.00,'2022-05-08'),
        ('Iris','Taylor','iris.taylor@enterprise.com','Dallas','TX','Enterprise',8200,128000.00,'2022-02-25'),
        ('James','Anderson','james.anderson@smb.com','San Jose','CA','SMB',4100,42000.00,'2022-03-18'),
        ('Karen','Thomas','karen.thomas@enterprise.com','Austin','TX','Enterprise',9400,158000.00,'2022-01-30'),
        ('Leo','Jackson','leo.jackson@smb.com','Jacksonville','FL','SMB',2800,22000.00,'2022-04-15'),
        ('Mia','White','mia.white@consumer.com','Columbus','OH','Consumer',650,1900.00,'2022-06-01'),
        ('Noah','Harris','noah.harris@smb.com','Fort Worth','TX','SMB',3600,31000.00,'2022-04-22'),
        ('Olivia','Martin','olivia.martin@enterprise.com','Charlotte','NC','Enterprise',7900,118000.00,'2022-03-08'),
        ('Peter','Garcia','peter.garcia@consumer.com','Indianapolis','IN','Consumer',1100,3800.00,'2022-05-15'),
        ('Quinn','Martinez','quinn.martinez@enterprise.com','San Francisco','CA','Enterprise',8600,138000.00,'2022-02-05'),
        ('Rachel','Robinson','rachel.robinson@smb.com','Seattle','WA','SMB',4300,45000.00,'2022-03-25'),
        ('Sam','Clark','sam.clark@enterprise.com','Denver','CO','Enterprise',7400,108000.00,'2022-04-10'),
        ('Tina','Rodriguez','tina.rodriguez@consumer.com','Nashville','TN','Consumer',780,2200.00,'2022-06-20'),
        ('Uriah','Lewis','uriah.lewis@smb.com','Oklahoma City','OK','SMB',2500,19000.00,'2022-05-01'),
        ('Vera','Lee','vera.lee@enterprise.com','El Paso','TX','Enterprise',8100,122000.00,'2022-03-15'),
        ('Wade','Walker','wade.walker@enterprise.com','Washington','DC','Enterprise',9200,148000.00,'2022-02-12'),
        ('Xena','Hall','xena.hall@smb.com','Las Vegas','NV','SMB',3100,26000.00,'2022-04-28'),
        ('Yara','Allen','yara.allen@consumer.com','Louisville','KY','Consumer',560,1600.00,'2022-07-05'),
        ('Zane','Young','zane.young@enterprise.com','Portland','OR','Enterprise',7700,112000.00,'2022-03-20'),
        ('Amy','Hernandez','amy.hernandez@smb.com','Memphis','TN','SMB',2900,24000.00,'2022-05-10'),
        ('Brad','King','brad.king@enterprise.com','Baltimore','MD','Enterprise',8400,131000.00,'2022-02-28'),
        ('Cora','Wright','cora.wright@consumer.com','Boston','MA','Consumer',1050,3400.00,'2022-06-15'),
        ('Dylan','Scott','dylan.scott@smb.com','Milwaukee','WI','SMB',3800,35000.00,'2022-04-05'),
        ('Elsa','Green','elsa.green@enterprise.com','Albuquerque','NM','Enterprise',7300,105000.00,'2022-03-30'),
        ('Felix','Adams','felix.adams@consumer.com','Tucson','AZ','Consumer',670,1800.00,'2022-07-12'),
        ('Gwen','Baker','gwen.baker@smb.com','Fresno','CA','SMB',2600,21000.00,'2022-05-20'),
        ('Hank','Gonzalez','hank.gonzalez@enterprise.com','Sacramento','CA','Enterprise',8700,136000.00,'2022-02-20'),
        ('Ida','Nelson','ida.nelson@consumer.com','Mesa','AZ','Consumer',820,2600.00,'2022-06-28'),
        ('Jack','Carter','jack.carter@smb.com','Atlanta','GA','SMB',4200,44000.00,'2022-04-18'),
        ('Kim','Mitchell','kim.mitchell@enterprise.com','Omaha','NE','Enterprise',7800,116000.00,'2022-03-05'),
        ('Liam','Perez','liam.perez@consumer.com','Raleigh','NC','Consumer',930,3100.00,'2022-07-20'),
        ('Mona','Roberts','mona.roberts@smb.com','Miami','FL','SMB',3300,29000.00,'2022-05-28'),
        ('Neil','Turner','neil.turner@enterprise.com','Minneapolis','MN','Enterprise',8300,129000.00,'2022-03-22'),
        ('Opal','Phillips','opal.phillips@consumer.com','Cleveland','OH','Consumer',720,2000.00,'2022-08-01'),
        ('Paul','Campbell','paul.campbell@smb.com','Tampa','FL','SMB',2700,23000.00,'2022-06-05'),
        ('Ria','Parker','ria.parker@enterprise.com','Honolulu','HI','Enterprise',9000,142000.00,'2022-02-15'),
        ('Stan','Evans','stan.evans@consumer.com','Anaheim','CA','Consumer',880,2900.00,'2022-07-28'),
        ('Tara','Edwards','tara.edwards@smb.com','Aurora','CO','SMB',3500,32000.00,'2022-05-15'),
        ('Uma','Collins','uma.collins@enterprise.com','Riverside','CA','Enterprise',7500,110000.00,'2022-04-02'),
        ('Val','Stewart','val.stewart@consumer.com','Lexington','KY','Consumer',610,1700.00,'2022-08-10'),
        ('Will','Sanchez','will.sanchez@smb.com','Corpus Christi','TX','SMB',3000,25000.00,'2022-06-12'),
        ('Xia','Morris','xia.morris@enterprise.com','Pittsburgh','PA','Enterprise',8000,120000.00,'2022-03-28'),
        ('Yuri','Rogers','yuri.rogers@consumer.com','Anchorage','AK','Consumer',750,2100.00,'2022-08-18'),
    ])

    # ── CAMPAIGNS ─────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO campaigns (campaign_name,channel,campaign_type,objective,budget,spend,status,start_date,end_date,target_audience) VALUES (?,?,?,?,?,?,?,?,?,?)", [
        ('Q1 2023 Brand Awareness Launch','Display','Awareness','Impressions',50000,48200,'completed','2023-01-01','2023-03-31','All Segments'),
        ('Q1 2023 Enterprise Lead Gen','Paid','Lead Gen','Leads',75000,72800,'completed','2023-01-15','2023-03-31','Enterprise'),
        ('Q1 2023 Nurture Email Series','Email','Nurture','Engagement',15000,14100,'completed','2023-02-01','2023-03-31','SMB'),
        ('Q2 2023 Spring Paid Search','Paid','Lead Gen','Conversions',60000,58900,'completed','2023-04-01','2023-06-30','Enterprise'),
        ('Q2 2023 Social Engagement','Social','Awareness','Engagement',25000,23400,'completed','2023-04-01','2023-06-30','Consumer'),
        ('Q2 2023 SEO Content Push','Organic','Awareness','Traffic',10000,9200,'completed','2023-04-15','2023-06-30','All Segments'),
        ('Q3 2023 Summer Email Blast','Email','Lead Gen','Leads',20000,19800,'completed','2023-07-01','2023-09-30','SMB'),
        ('Q3 2023 Retargeting Push','Paid','Retargeting','Conversions',35000,33200,'completed','2023-07-15','2023-09-30','Enterprise'),
        ('Q3 2023 LinkedIn Awareness','Social','Awareness','Impressions',18000,17100,'completed','2023-08-01','2023-09-30','Enterprise'),
        ('Q4 2023 Holiday Retargeting','Paid','Retargeting','Conversions',45000,44200,'completed','2023-10-01','2023-12-31','All Segments'),
        ('Q4 2023 Year-End Nurture','Email','Nurture','Retention',12000,11600,'completed','2023-10-15','2023-12-31','Enterprise'),
        ('Q4 2023 Partner Referral Drive','Referral','Lead Gen','Leads',8000,7400,'completed','2023-11-01','2023-12-31','SMB'),
        ('Q1 2024 Enterprise Account-Based','Paid','Lead Gen','Pipeline',90000,88500,'completed','2024-01-01','2024-03-31','Enterprise'),
        ('Q1 2024 SMB Webinar Series','Email','Nurture','Engagement',22000,21200,'completed','2024-01-15','2024-03-31','SMB'),
        ('Q1 2024 Organic Blog Expansion','Organic','Awareness','Traffic',12000,11800,'completed','2024-02-01','2024-03-31','All Segments'),
        ('Q1 2024 Display Retargeting','Display','Retargeting','Conversions',30000,28900,'completed','2024-01-01','2024-03-31','All Segments'),
        ('Q2 2024 Paid Search Scale-Up','Paid','Lead Gen','Conversions',80000,78200,'completed','2024-04-01','2024-06-30','Enterprise'),
        ('Q2 2024 Partner Referral Program','Referral','Lead Gen','Leads',15000,14600,'completed','2024-04-01','2024-06-30','SMB'),
        ('Q2 2024 LinkedIn ABM','Social','Lead Gen','Pipeline',40000,38800,'completed','2024-04-15','2024-06-30','Enterprise'),
        ('Q2 2024 Nurture Reengagement','Email','Nurture','Retention',18000,17400,'completed','2024-04-01','2024-06-30','SMB'),
        ('Q3 2024 Summer SEO Push','Organic','Awareness','Traffic',14000,13600,'completed','2024-07-01','2024-09-30','All Segments'),
        ('Q3 2024 Paid Search Expansion','Paid','Lead Gen','Conversions',95000,93400,'completed','2024-07-01','2024-09-30','Enterprise'),
        ('Q3 2024 Social Retargeting','Social','Retargeting','Conversions',28000,27100,'completed','2024-07-15','2024-09-30','All Segments'),
        ('Q3 2024 Enterprise Nurture','Email','Nurture','Retention',20000,19500,'completed','2024-07-01','2024-09-30','Enterprise'),
        ('Q4 2024 Black Friday Paid','Paid','Lead Gen','Conversions',110000,108200,'completed','2024-10-01','2024-12-31','All Segments'),
        ('Q4 2024 Holiday Email Sequence','Email','Nurture','Retention',25000,24300,'completed','2024-10-15','2024-12-31','Enterprise'),
        ('Q4 2024 Year-End Referral Push','Referral','Lead Gen','Leads',20000,19200,'completed','2024-11-01','2024-12-31','SMB'),
        ('Q4 2024 Display Brand Blitz','Display','Awareness','Impressions',45000,43800,'completed','2024-10-01','2024-12-31','All Segments'),
        ('Q4 2024 Paid Social LinkedIn','Social','Lead Gen','Pipeline',55000,53600,'completed','2024-10-01','2024-12-31','Enterprise'),
        ('Q1 2025 New Year Enterprise Push','Paid','Lead Gen','Pipeline',100000,45200,'active','2025-01-01','2025-03-31','Enterprise'),
        ('Q1 2025 Email Re-engagement','Email','Nurture','Retention',20000,9800,'active','2025-01-15','2025-03-31','SMB'),
        ('Q1 2025 SEO Authority Build','Organic','Awareness','Traffic',15000,7100,'active','2025-02-01','2025-03-31','All Segments'),
        ('Q1 2025 Social Brand Presence','Social','Awareness','Engagement',30000,14200,'paused','2025-01-01','2025-03-31','Consumer'),
        ('Q1 2025 Display Prospecting','Display','Awareness','Impressions',35000,16800,'active','2025-01-15','2025-03-31','SMB'),
        ('Q2 2025 Spring Paid Launch','Paid','Lead Gen','Conversions',120000,0,'planned','2025-04-01','2025-06-30','Enterprise'),
        ('Q2 2025 Partner Expansion','Referral','Lead Gen','Leads',25000,0,'planned','2025-04-01','2025-06-30','SMB'),
        ('Q2 2025 Content Marketing Push','Organic','Awareness','Traffic',16000,0,'draft','2025-05-01','2025-06-30','All Segments'),
        ('Q2 2025 LinkedIn Account-Based','Social','Lead Gen','Pipeline',60000,0,'planned','2025-04-15','2025-06-30','Enterprise'),
        ('Q2 2025 Email Win-Back','Email','Nurture','Retention',18000,0,'draft','2025-04-01','2025-06-30','SMB'),
        ('Q2 2025 Display Retargeting','Display','Retargeting','Conversions',40000,0,'planned','2025-04-01','2025-06-30','All Segments'),
    ])

    # ── LEADS ─────────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO leads (campaign_id,customer_id,email,first_name,last_name,lead_source,status,deal_value,score,created_at,converted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
        (1,None,'display1q1_23@prospect.com','Aaron','Blake','Display','qualified',None,68,'2023-01-15',None),
        (1,None,'display2q1_23@prospect.com','Bella','Chase','Display','stale',None,22,'2023-01-20',None),
        (1,None,'display3q1_23@prospect.com','Cody','Drake','Display','new',None,None,'2023-02-01',None),
        (2,None,'paid1q1_23@prospect.com','Fiona','Grant','Paid','converted',35000,91,'2023-01-18','2023-02-18'),
        (2,None,'paid2q1_23@prospect.com','Gary','Hayes','Paid','converted',52000,96,'2023-01-22','2023-02-22'),
        (2,None,'paid3q1_23@prospect.com','Hana','Irwin','Paid','converted',28000,88,'2023-01-25','2023-02-25'),
        (2,None,'paid4q1_23@prospect.com','Ian','James','Paid','qualified',None,72,'2023-02-01',None),
        (2,None,'paid5q1_23@prospect.com','Jade','Kim','Paid','qualified',None,65,'2023-02-05',None),
        (2,None,'paid6q1_23@prospect.com','Kyle','Lane','Paid','stale',None,35,'2023-02-08',None),
        (2,None,'paid7q1_23@prospect.com','Lara','Moon','Paid','converted',67000,98,'2023-02-12','2023-03-12'),
        (2,None,'paid8q1_23@prospect.com','Mike','Nash','Paid','new',None,None,'2023-02-18',None),
        (2,None,'paid9q1_23@prospect.com','Nina','Owen','Paid','converted',44000,94,'2023-02-22','2023-03-22'),
        (3,None,'email1q1_23@prospect.com','Penny','Quinn','Email','converted',18000,85,'2023-02-05','2023-03-05'),
        (3,None,'email2q1_23@prospect.com','Rex','Reed','Email','converted',24000,89,'2023-02-10','2023-03-10'),
        (3,None,'email3q1_23@prospect.com','Sara','Stone','Email','qualified',None,70,'2023-02-15',None),
        (3,None,'email6q1_23@prospect.com','Vera','West','Email','converted',31000,92,'2023-03-01','2023-04-01'),
        (4,None,'paid1q2_23@prospect.com','Yara','Zane','Paid','converted',41000,93,'2023-04-05','2023-05-05'),
        (4,None,'paid2q2_23@prospect.com','Zack','Abel','Paid','converted',63000,97,'2023-04-08','2023-05-08'),
        (4,None,'paid3q2_23@prospect.com','Amy','Brown','Paid','qualified',None,74,'2023-04-12',None),
        (4,None,'paid4q2_23@prospect.com','Bob','Clark','Paid','converted',29000,87,'2023-04-15','2023-05-15'),
        (4,None,'paid7q2_23@prospect.com','Eve','Fern','Paid','converted',48000,95,'2023-04-25','2023-05-25'),
        (7,None,'email1q3_23@prospect.com','Pat','Quinn','Email','converted',22000,87,'2023-07-05','2023-08-05'),
        (7,None,'email2q3_23@prospect.com','Rea','Ross','Email','converted',15000,83,'2023-07-08','2023-08-08'),
        (7,None,'email5q3_23@prospect.com','Uly','Upton','Email','converted',34000,91,'2023-07-22','2023-08-22'),
        (8,None,'paid1q3_23@prospect.com','Xia','Xcel','Paid','converted',55000,96,'2023-07-10','2023-08-10'),
        (8,None,'paid2q3_23@prospect.com','Yul','Yale','Paid','converted',38000,92,'2023-07-15','2023-08-15'),
        (8,None,'paid4q3_23@prospect.com','Ann','Abel','Paid','converted',71000,98,'2023-07-25','2023-08-25'),
        (10,None,'paid1q4_23@prospect.com','Hew','Hart','Paid','converted',47000,94,'2023-10-05','2023-11-05'),
        (10,None,'paid2q4_23@prospect.com','Ida','Ives','Paid','converted',62000,97,'2023-10-08','2023-11-08'),
        (10,None,'paid4q4_23@prospect.com','Jan','Kent','Paid','converted',29000,88,'2023-10-15','2023-11-15'),
        (10,None,'paid7q4_23@prospect.com','Kay','Nash','Paid','converted',53000,96,'2023-10-25','2023-11-25'),
        (11,None,'email1q4_23@prospect.com','Luz','Park','Email','converted',19500,86,'2023-10-15','2023-11-15'),
        (11,None,'email2q4_23@prospect.com','Mae','Quinn','Email','converted',27000,90,'2023-10-20','2023-11-20'),
        (12,None,'ref1q4_23@prospect.com','Mel','Stone','Referral','converted',38000,93,'2023-11-05','2023-12-05'),
        (12,None,'ref2q4_23@prospect.com','Mia','Todd','Referral','converted',51000,97,'2023-11-10','2023-12-10'),
        (13,None,'paid1q1_24@prospect.com','Nat','Wade','Paid','converted',78000,99,'2024-01-10','2024-02-10'),
        (13,None,'paid2q1_24@prospect.com','Nel','Xcel','Paid','converted',55000,95,'2024-01-15','2024-02-15'),
        (13,None,'paid3q1_24@prospect.com','Ned','Yale','Paid','converted',34000,90,'2024-01-20','2024-02-20'),
        (13,None,'paid7q1_24@prospect.com','Nov','Cain','Paid','converted',91000,99,'2024-02-10','2024-03-10'),
        (13,None,'paid9q1_24@prospect.com','Ola','Earl','Paid','converted',62000,96,'2024-02-20','2024-03-20'),
        (14,None,'email1q1_24@prospect.com','Oma','Gray','Email','converted',14000,84,'2024-01-20','2024-02-20'),
        (14,None,'email2q1_24@prospect.com','Ona','Hart','Email','converted',21000,88,'2024-01-25','2024-02-25'),
        (14,None,'email6q1_24@prospect.com','Owen','Lane','Email','converted',33000,92,'2024-02-15','2024-03-15'),
        (17,None,'paid1q2_24@prospect.com','Pix','Upton','Paid','converted',84000,99,'2024-04-05','2024-05-05'),
        (17,None,'paid2q2_24@prospect.com','Poe','Vane','Paid','converted',61000,95,'2024-04-08','2024-05-08'),
        (17,None,'paid3q2_24@prospect.com','Pol','Wade','Paid','converted',39000,91,'2024-04-12','2024-05-12'),
        (17,None,'paid7q2_24@prospect.com','Psi','Abel','Paid','converted',97000,99,'2024-04-25','2024-05-25'),
        (17,None,'paid9q2_24@prospect.com','Pug','Cain','Paid','converted',52000,94,'2024-05-05','2024-06-05'),
        (17,None,'paid11q2_24@prospect.com','Pup','Earl','Paid','converted',43000,93,'2024-05-15','2024-06-15'),
        (18,None,'ref1q2_24@prospect.com','Qua','Gray','Referral','converted',26000,92,'2024-04-10','2024-05-10'),
        (18,None,'ref2q2_24@prospect.com','Que','Hart','Referral','converted',54000,97,'2024-04-15','2024-05-15'),
        (18,None,'ref3q2_24@prospect.com','Qui','Ives','Referral','converted',12000,84,'2024-04-20','2024-05-20'),
        (18,None,'ref6q2_24@prospect.com','Rag','Lane','Referral','converted',31000,94,'2024-05-08','2024-06-08'),
        (18,None,'ref7q2_24@prospect.com','Ram','Marsh','Referral','converted',47000,96,'2024-05-12','2024-06-12'),
        (19,None,'social1q2_24@prospect.com','Ran','Nash','Social','converted',58000,97,'2024-04-18','2024-05-18'),
        (19,None,'social2q2_24@prospect.com','Rap','Owen','Social','converted',72000,99,'2024-04-22','2024-05-22'),
        (19,None,'social7q2_24@prospect.com','Reb','Shaw','Social','converted',44000,95,'2024-05-15','2024-06-15'),
        (20,None,'email1q2_24@prospect.com','Red','Stone','Email','converted',17500,88,'2024-04-05','2024-05-05'),
        (20,None,'email2q2_24@prospect.com','Reg','Todd','Email','converted',44000,95,'2024-04-10','2024-05-10'),
        (20,None,'email5q2_24@prospect.com','Rey','Wade','Email','converted',25000,91,'2024-04-25','2024-05-25'),
        (20,None,'email7q2_24@prospect.com','Ric','Yale','Email','converted',48000,96,'2024-05-05','2024-06-05'),
        (22,None,'paid1q3_24@prospect.com','Rue','Earl','Paid','converted',89000,99,'2024-07-08','2024-08-08'),
        (22,None,'paid2q3_24@prospect.com','Rum','Ford','Paid','converted',66000,96,'2024-07-12','2024-08-12'),
        (22,None,'paid3q3_24@prospect.com','Run','Gray','Paid','converted',41000,92,'2024-07-15','2024-08-15'),
        (22,None,'paid7q3_24@prospect.com','Sab','Kent','Paid','converted',102000,99,'2024-07-28','2024-08-28'),
        (22,None,'paid9q3_24@prospect.com','Sad','Marsh','Paid','converted',57000,95,'2024-08-05','2024-09-05'),
        (22,None,'paid11q3_24@prospect.com','Sal','Owen','Paid','converted',48000,94,'2024-08-12','2024-09-12'),
        (23,None,'social1q3_24@prospect.com','San','Quinn','Social','converted',35000,92,'2024-07-18','2024-08-18'),
        (23,None,'social2q3_24@prospect.com','Sap','Reed','Social','converted',51000,95,'2024-07-22','2024-08-22'),
        (24,None,'email1q3_24@prospect.com','Sea','Todd','Email','converted',10500,83,'2024-07-05','2024-08-01'),
        (24,None,'email2q3_24@prospect.com','Sec','Upton','Email','converted',15000,87,'2024-07-08','2024-08-08'),
        (24,None,'email3q3_24@prospect.com','Sed','Vane','Email','converted',28000,93,'2024-07-10','2024-08-15'),
        (24,None,'email7q3_24@prospect.com','Sew','Zane','Email','converted',39000,94,'2024-07-22','2024-08-22'),
        (25,None,'paid1q4_24@prospect.com','Shi','Blue','Paid','converted',93000,99,'2024-10-05','2024-11-05'),
        (25,None,'paid2q4_24@prospect.com','Sho','Cain','Paid','converted',74000,97,'2024-10-08','2024-11-08'),
        (25,None,'paid3q4_24@prospect.com','Shy','Dean','Paid','converted',45000,93,'2024-10-12','2024-11-12'),
        (25,None,'paid7q4_24@prospect.com','Sip','Hart','Paid','converted',108000,99,'2024-10-25','2024-11-25'),
        (25,None,'paid9q4_24@prospect.com','Sit','Jack','Paid','converted',67000,96,'2024-11-05','2024-12-05'),
        (25,None,'paid11q4_24@prospect.com','Sky','Lane','Paid','converted',55000,95,'2024-11-12','2024-12-12'),
        (25,None,'paid12q4_24@prospect.com','Sly','Marsh','Paid','converted',33000,94,'2024-11-15','2024-12-15'),
        (26,None,'email1q4_24@prospect.com','Sod','Owen','Email','converted',21000,88,'2024-10-18','2024-11-18'),
        (26,None,'email2q4_24@prospect.com','Sol','Park','Email','converted',38000,93,'2024-10-22','2024-11-22'),
        (26,None,'email6q4_24@prospect.com','Sow','Shaw','Email','converted',54000,96,'2024-11-05','2024-12-05'),
        (27,None,'ref1q4_24@prospect.com','Spa','Upton','Referral','converted',42000,94,'2024-11-05','2024-12-05'),
        (27,None,'ref2q4_24@prospect.com','Spe','Vane','Referral','converted',68000,98,'2024-11-08','2024-12-08'),
        (27,None,'ref5q4_24@prospect.com','Spu','Yale','Referral','converted',29000,90,'2024-11-18','2024-12-18'),
        (29,None,'social1q4_24@prospect.com','Sun','Earl','Social','converted',61000,97,'2024-10-10','2024-11-10'),
        (29,None,'social2q4_24@prospect.com','Sup','Ford','Social','converted',82000,99,'2024-10-15','2024-11-15'),
        (29,None,'social7q4_24@prospect.com','Suz','Kent','Social','converted',49000,95,'2024-11-01','2024-12-01'),
        (29,None,'social8q4_24@prospect.com','Tab','Lane','Social','converted',71000,98,'2024-11-05','2024-12-05'),
        (29,None,'social9q4_24@prospect.com','Tad','Marsh','Social','converted',38000,93,'2024-11-08','2024-12-08'),
        (30,None,'paid3q1_25@prospect.com','Tar','Quinn','Paid','converted',58000,95,'2025-01-22','2025-02-22'),
        (30,None,'paid5q1_25@prospect.com','Tax','Ross','Paid','converted',42000,91,'2025-02-05','2025-03-05'),
        (31,None,'email2q1_25@prospect.com','Ted','Stone','Email','converted',24000,87,'2025-01-25','2025-02-25'),
        # non-converted leads
        (1,None,'display4q1_23@prospect.com','Dina','Ellis','Display','qualified',None,55,'2023-02-10',None),
        (4,None,'paid5q2_23@prospect.com','Cara','Duke','Paid','stale',None,32,'2023-04-18',None),
        (5,None,'social1q2_23@prospect.com','Gail','Hart','Social','qualified',None,62,'2023-04-08',None),
        (5,None,'social2q2_23@prospect.com','Hank','Irma','Social','stale',None,27,'2023-04-12',None),
        (9,None,'social1q3_23@prospect.com','Dee','Dean','Social','qualified',None,65,'2023-08-05',None),
        (13,None,'paid4q1_24@prospect.com','Nia','Zane','Paid','qualified',None,78,'2024-01-25',None),
        (13,None,'paid5q1_24@prospect.com','Noe','Abel','Paid','qualified',None,73,'2024-02-01',None),
        (17,None,'paid4q2_24@prospect.com','Pop','Xcel','Paid','qualified',None,80,'2024-04-15',None),
        (22,None,'paid4q3_24@prospect.com','Rut','Hart','Paid','qualified',None,82,'2024-07-18',None),
        (25,None,'paid4q4_24@prospect.com','Sid','Earl','Paid','qualified',None,83,'2024-10-15',None),
        (25,None,'paid8q4_24@prospect.com','Sir','Ives','Paid','new',None,None,'2024-11-01',None),
        (30,None,'paid1q1_25@prospect.com','Tan','Owen','Paid','qualified',None,79,'2025-01-12',None),
        (31,None,'email1q1_25@prospect.com','Tea','Shaw','Email','qualified',None,68,'2025-01-20',None),
    ])

    # ── ORDERS ────────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO orders (customer_id,campaign_id,order_date,shipped_date,status,amount) VALUES (?,?,?,?,?,?)", [
        (1,1,'2023-02-10','2023-02-14','delivered',12500),
        (2,1,'2023-02-15','2023-02-19','delivered',4200),
        (3,2,'2023-02-12','2023-02-16','delivered',28000),
        (4,1,'2023-03-01','2023-03-05','delivered',1800),
        (5,2,'2023-02-05','2023-02-09','delivered',45000),
        (7,2,'2023-02-08','2023-02-12','delivered',18500),
        (9,2,'2023-02-12','2023-02-16','delivered',62000),
        (11,2,'2023-02-18','2023-02-22','delivered',22000),
        (12,2,'2023-03-05','2023-03-09','delivered',14000),
        (15,3,'2023-02-20','2023-02-24','delivered',31000),
        (17,3,'2023-02-25','2023-03-01','delivered',19500),
        (19,3,'2023-03-01','2023-03-05','delivered',52000),
        (22,3,'2023-03-10','2023-03-14','delivered',23000),
        (23,4,'2023-04-10','2023-04-14','delivered',41000),
        (24,4,'2023-04-15','2023-04-19','delivered',63000),
        (25,4,'2023-04-20','2023-04-24','delivered',29000),
        (26,4,'2023-04-25','2023-04-29','delivered',48000),
        (3,7,'2023-07-15','2023-07-19','delivered',22000),
        (5,7,'2023-07-20','2023-07-24','delivered',15000),
        (7,7,'2023-07-25','2023-07-29','delivered',34000),
        (19,8,'2023-07-18','2023-07-22','delivered',55000),
        (22,8,'2023-07-22','2023-07-26','delivered',38000),
        (23,8,'2023-07-28','2023-08-01','delivered',71000),
        (9,10,'2023-10-08','2023-10-12','delivered',47000),
        (11,10,'2023-10-12','2023-10-16','delivered',62000),
        (15,10,'2023-10-18','2023-10-22','delivered',29000),
        (17,10,'2023-10-22','2023-10-26','delivered',53000),
        (1,11,'2023-10-20','2023-10-24','delivered',19500),
        (2,11,'2023-10-25','2023-10-29','delivered',27000),
        (3,12,'2023-11-10','2023-11-14','delivered',38000),
        (5,12,'2023-11-15','2023-11-19','delivered',51000),
        (4,13,'2024-01-15','2024-01-19','delivered',78000),
        (6,13,'2024-01-20','2024-01-24','delivered',55000),
        (8,13,'2024-01-25','2024-01-29','delivered',34000),
        (10,13,'2024-02-05','2024-02-09','delivered',91000),
        (12,13,'2024-02-12','2024-02-16','delivered',62000),
        (14,14,'2024-01-25','2024-01-29','delivered',14000),
        (16,14,'2024-02-01','2024-02-05','delivered',21000),
        (18,14,'2024-02-10','2024-02-14','delivered',33000),
        (7,17,'2024-04-08','2024-04-12','delivered',84000),
        (9,17,'2024-04-12','2024-04-16','delivered',61000),
        (11,17,'2024-04-18','2024-04-22','delivered',39000),
        (13,17,'2024-04-25','2024-04-29','delivered',97000),
        (15,17,'2024-05-08','2024-05-12','delivered',52000),
        (17,17,'2024-05-15','2024-05-19','delivered',43000),
        (19,18,'2024-04-15','2024-04-19','delivered',26000),
        (21,18,'2024-04-20','2024-04-24','delivered',54000),
        (23,18,'2024-04-25','2024-04-29','delivered',12000),
        (25,18,'2024-05-10','2024-05-14','delivered',31000),
        (27,18,'2024-05-15','2024-05-19','delivered',47000),
        (1,19,'2024-04-22','2024-04-26','delivered',58000),
        (3,19,'2024-04-26','2024-04-30','delivered',72000),
        (5,19,'2024-05-20','2024-05-24','delivered',44000),
        (15,22,'2024-07-10','2024-07-14','delivered',89000),
        (17,22,'2024-07-15','2024-07-19','delivered',66000),
        (19,22,'2024-07-18','2024-07-22','delivered',41000),
        (21,22,'2024-07-30','2024-08-03','delivered',102000),
        (23,22,'2024-08-08','2024-08-12','delivered',57000),
        (25,22,'2024-08-15','2024-08-19','delivered',48000),
        (31,24,'2024-07-08','2024-07-12','delivered',10500),
        (33,24,'2024-07-12','2024-07-16','delivered',15000),
        (35,24,'2024-07-18','2024-07-22','delivered',28000),
        (37,24,'2024-07-25','2024-07-29','delivered',39000),
        (39,25,'2024-10-08','2024-10-12','delivered',93000),
        (41,25,'2024-10-12','2024-10-16','delivered',74000),
        (43,25,'2024-10-15','2024-10-19','delivered',45000),
        (45,25,'2024-10-28','2024-11-01','delivered',108000),
        (1,25,'2024-11-08','2024-11-12','delivered',67000),
        (3,25,'2024-11-12','2024-11-16','delivered',55000),
        (5,25,'2024-11-15','2024-11-19','delivered',33000),
        (7,26,'2024-10-20','2024-10-24','delivered',21000),
        (9,26,'2024-10-25','2024-10-29','delivered',38000),
        (11,26,'2024-11-08','2024-11-12','delivered',54000),
        (13,27,'2024-11-08','2024-11-12','delivered',42000),
        (15,27,'2024-11-12','2024-11-16','delivered',68000),
        (17,27,'2024-11-20','2024-11-24','delivered',29000),
        (19,29,'2024-10-12','2024-10-16','delivered',61000),
        (21,29,'2024-10-18','2024-10-22','delivered',82000),
        (23,29,'2024-11-05','2024-11-09','delivered',49000),
        (25,29,'2024-11-10','2024-11-14','delivered',71000),
        (27,29,'2024-11-12','2024-11-16','delivered',38000),
        (1,30,'2025-02-25','2025-03-01','delivered',58000),
        (3,30,'2025-03-08','2025-03-12','delivered',42000),
        (2,31,'2025-02-28','2025-03-04','delivered',24000),
    ])

    # ── ORDER ITEMS ───────────────────────────────────────────────────────────
    order_items_data = []
    order_product_map = [
        (1,[3,14,16,9]),(2,[2,15,17]),(3,[3,14,16,12]),(4,[1,17,9]),(5,[3,14,16,12]),
        (6,[3,13,15,8]),(7,[3,14,16,12]),(8,[3,13,16,11]),(9,[2,13,15,6]),(10,[3,13,16,15]),
        (11,[3,13,15,8]),(12,[3,14,16,10]),(13,[3,14,16]),(14,[3,14,12]),(15,[3,13,15]),
        (16,[3,14,16]),(17,[2,15,17]),(18,[1,17,9]),(19,[3,13,15,8]),(20,[2,17,9]),
        (21,[3,13,15]),(22,[3,12,16]),(23,[3,14,16,12]),(24,[3,14,16,11]),(25,[3,14,16,12]),
        (26,[3,14,12]),(27,[3,14,16]),(28,[3,13,15,8]),(29,[2,15,17]),(30,[3,14,16,12]),
        (31,[3,13,16,11]),(32,[3,14,16,10]),
    ]
    for order_id, products in order_product_map:
        for pid in products:
            price = {1:299,2:799,3:1999,6:599,8:499,9:800,10:2400,11:1200,12:3500,13:1800,14:6000,15:1200,16:2400,17:299}.get(pid,499)
            order_items_data.append((order_id, pid, 1, price))
    c.executemany("INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (?,?,?,?)", order_items_data)

    # ── PAYMENTS ──────────────────────────────────────────────────────────────
    payments_data = [
        (1,1,12500,'2023-02-10'),(2,2,4200,'2023-02-15'),(3,3,28000,'2023-02-12'),
        (4,4,1800,'2023-03-01'),(5,5,45000,'2023-02-05'),(7,6,18500,'2023-02-08'),
        (9,7,62000,'2023-02-12'),(11,8,22000,'2023-02-18'),(12,9,14000,'2023-03-05'),
        (15,10,31000,'2023-02-20'),(17,11,19500,'2023-02-25'),(19,12,52000,'2023-03-01'),
        (22,13,23000,'2023-03-10'),(23,14,41000,'2023-04-10'),(24,15,63000,'2023-04-15'),
        (25,16,29000,'2023-04-20'),(26,17,48000,'2023-04-25'),
        (3,18,22000,'2023-07-15'),(5,19,15000,'2023-07-20'),(7,20,34000,'2023-07-25'),
        (19,21,55000,'2023-07-18'),(22,22,38000,'2023-07-22'),(23,23,71000,'2023-07-28'),
        (9,24,47000,'2023-10-08'),(11,25,62000,'2023-10-12'),(15,26,29000,'2023-10-18'),
        (17,27,53000,'2023-10-22'),(1,28,19500,'2023-10-20'),(2,29,27000,'2023-10-25'),
        (3,30,38000,'2023-11-10'),(5,31,51000,'2023-11-15'),
        (4,32,78000,'2024-01-15'),(6,33,55000,'2024-01-20'),(8,34,34000,'2024-01-25'),
        (10,35,91000,'2024-02-05'),(12,36,62000,'2024-02-12'),
        (14,37,14000,'2024-01-25'),(16,38,21000,'2024-02-01'),(18,39,33000,'2024-02-10'),
        (7,40,84000,'2024-04-08'),(9,41,61000,'2024-04-12'),(11,42,39000,'2024-04-18'),
        (13,43,97000,'2024-04-25'),(15,44,52000,'2024-05-08'),(17,45,43000,'2024-05-15'),
        (19,46,26000,'2024-04-15'),(21,47,54000,'2024-04-20'),(23,48,12000,'2024-04-25'),
        (25,49,31000,'2024-05-10'),(27,50,47000,'2024-05-15'),
        (1,51,58000,'2024-04-22'),(3,52,72000,'2024-04-26'),(5,53,44000,'2024-05-20'),
        (15,54,89000,'2024-07-10'),(17,55,66000,'2024-07-15'),(19,56,41000,'2024-07-18'),
        (21,57,102000,'2024-07-30'),(23,58,57000,'2024-08-08'),(25,59,48000,'2024-08-15'),
        (31,60,10500,'2024-07-08'),(33,61,15000,'2024-07-12'),(35,62,28000,'2024-07-18'),
        (37,63,39000,'2024-07-25'),
        (39,64,93000,'2024-10-08'),(41,65,74000,'2024-10-12'),(43,66,45000,'2024-10-15'),
        (45,67,108000,'2024-10-28'),(1,68,67000,'2024-11-08'),(3,69,55000,'2024-11-12'),
        (5,70,33000,'2024-11-15'),(7,71,21000,'2024-10-20'),(9,72,38000,'2024-10-25'),
        (11,73,54000,'2024-11-08'),(13,74,42000,'2024-11-08'),(15,75,68000,'2024-11-12'),
        (17,76,29000,'2024-11-20'),(19,77,61000,'2024-10-12'),(21,78,82000,'2024-10-18'),
        (23,79,49000,'2024-11-05'),(25,80,71000,'2024-11-10'),(27,81,38000,'2024-11-12'),
        (1,82,58000,'2025-02-25'),(3,83,42000,'2025-03-08'),(2,84,24000,'2025-02-28'),
    ]
    c.executemany("INSERT INTO payments (customer_id,order_id,amount,paid_at) VALUES (?,?,?,?)", payments_data)

    # ── SEO KEYWORDS ──────────────────────────────────────────────────────────
    c.executemany("INSERT INTO seo_keywords (keyword,search_volume,keyword_difficulty,intent_type,topic_cluster) VALUES (?,?,?,?,?)", [
        ('what is marketing automation',18100,38,'informational','Marketing Automation'),
        ('marketing automation examples',5400,32,'informational','Marketing Automation'),
        ('marketing automation benefits',3600,28,'informational','Marketing Automation'),
        ('how does marketing automation work',2900,30,'informational','Marketing Automation'),
        ('marketing automation for small business',2400,25,'informational','Marketing Automation'),
        ('email marketing automation',9900,42,'informational','Email Marketing'),
        ('email drip campaigns',6600,36,'informational','Email Marketing'),
        ('email marketing best practices',8100,40,'informational','Email Marketing'),
        ('what is marketing automation software',4400,35,'informational','Marketing Automation'),
        ('marketing automation workflow',3300,33,'informational','Marketing Automation'),
        ('best marketing automation software',12100,62,'commercial','Marketing Automation'),
        ('marketing automation tools comparison',4400,55,'commercial','Marketing Automation'),
        ('marketing automation pricing',3600,48,'commercial','Marketing Automation'),
        ('HubSpot vs Marketo',5400,58,'commercial','Competitive'),
        ('marketing automation platform review',2900,52,'commercial','Marketing Automation'),
        ('crm marketing integration',6600,54,'commercial','CRM Integration'),
        ('crm for marketing teams',4400,50,'commercial','CRM Integration'),
        ('marketing crm software',5400,52,'commercial','CRM Integration'),
        ('content marketing strategy',9900,44,'informational','Content Marketing'),
        ('content marketing ROI',4400,40,'informational','Content Marketing'),
        ('marketing automation software',14800,68,'transactional','Marketing Automation'),
        ('buy marketing automation tool',1600,62,'transactional','Marketing Automation'),
        ('marketing automation demo',2900,58,'transactional','Marketing Automation'),
        ('marketing automation free trial',2200,55,'transactional','Marketing Automation'),
        ('marketing automation implementation',1900,50,'transactional','Marketing Automation'),
        ('seo analytics tool',5400,46,'transactional','SEO Analytics'),
        ('seo reporting software',3600,44,'transactional','SEO Analytics'),
        ('social media management tool',8100,50,'transactional','Social Media'),
        ('attribution software',3300,48,'transactional','Analytics'),
        ('marketing analytics platform',4400,52,'transactional','Analytics'),
        ('marketing automation ROI calculator',1900,42,'informational','Marketing Automation'),
        ('lead scoring marketing automation',2900,46,'informational','Lead Management'),
        ('marketing qualified lead definition',3600,38,'informational','Lead Management'),
        ('b2b lead generation tactics',6600,48,'informational','Lead Management'),
        ('marketing funnel stages',8100,36,'informational','Funnel Analytics'),
        ('customer journey mapping',4400,40,'informational','Funnel Analytics'),
        ('multi-touch attribution marketing',3300,52,'informational','Analytics'),
        ('marketing dashboard examples',2900,38,'informational','Analytics'),
        ('digital marketing trends',12100,44,'informational','Trends'),
        ('marketing automation trends',4400,42,'informational','Marketing Automation'),
    ])

    # ── SEO RANKINGS ──────────────────────────────────────────────────────────
    c.executemany("INSERT INTO seo_rankings (keyword_id,ranking_date,position,page_url,impressions,clicks,ctr_pct) VALUES (?,?,?,?,?,?,?)", [
        (1,'2023-01-01',28,'/blog/what-is-marketing-automation',18100,54,0.30),
        (1,'2023-04-01',16,'/blog/what-is-marketing-automation',18100,181,1.00),
        (1,'2023-07-01',9,'/blog/what-is-marketing-automation',18100,470,2.60),
        (1,'2023-10-01',5,'/blog/what-is-marketing-automation',18100,869,4.80),
        (1,'2024-01-01',3,'/blog/what-is-marketing-automation',18100,1448,8.00),
        (1,'2024-04-01',2,'/blog/what-is-marketing-automation',18100,2171,12.00),
        (1,'2024-07-01',1,'/blog/what-is-marketing-automation',18100,3348,18.50),
        (1,'2024-10-01',1,'/blog/what-is-marketing-automation',18100,4163,23.00),
        (9,'2023-01-01',38,'/blog/what-is-marketing-automation',4400,9,0.20),
        (9,'2023-04-01',24,'/blog/what-is-marketing-automation',4400,53,1.20),
        (9,'2023-07-01',14,'/blog/what-is-marketing-automation',4400,141,3.20),
        (9,'2023-10-01',8,'/blog/what-is-marketing-automation',4400,290,6.60),
        (9,'2024-01-01',5,'/blog/what-is-marketing-automation',4400,484,11.00),
        (9,'2024-04-01',3,'/blog/what-is-marketing-automation',4400,704,16.00),
        (9,'2024-07-01',2,'/blog/what-is-marketing-automation',4400,924,21.00),
        (9,'2024-10-01',1,'/blog/what-is-marketing-automation',4400,1232,28.00),
        (19,'2023-01-01',32,'/blog/content-marketing-strategy',9900,30,0.30),
        (19,'2023-04-01',20,'/blog/content-marketing-strategy',9900,198,2.00),
        (19,'2023-07-01',11,'/blog/content-marketing-strategy',9900,545,5.50),
        (19,'2023-10-01',8,'/blog/content-marketing-strategy',9900,891,9.00),
        (19,'2024-01-01',6,'/blog/content-marketing-strategy',9900,1188,12.00),
        (19,'2024-04-01',4,'/blog/content-marketing-strategy',9900,1782,18.00),
        (19,'2024-07-01',3,'/blog/content-marketing-strategy',9900,2277,23.00),
        (19,'2024-10-01',2,'/blog/content-marketing-strategy',9900,3069,31.00),
        (39,'2023-01-01',45,'/blog/digital-marketing-trends',12100,24,0.20),
        (39,'2023-04-01',28,'/blog/digital-marketing-trends',12100,255,2.10),
        (39,'2023-07-01',15,'/blog/digital-marketing-trends',12100,727,6.00),
        (39,'2023-10-01',10,'/blog/digital-marketing-trends',12100,1331,11.00),
        (39,'2024-01-01',7,'/blog/digital-marketing-trends',12100,1694,14.00),
        (39,'2024-04-01',5,'/blog/digital-marketing-trends',12100,2420,20.00),
        (39,'2024-07-01',4,'/blog/digital-marketing-trends',12100,3388,28.00),
        (39,'2024-10-01',3,'/blog/digital-marketing-trends',12100,4840,40.00),
    ])

    # ── ORGANIC TRAFFIC ───────────────────────────────────────────────────────
    c.executemany("INSERT INTO organic_traffic (traffic_date,page_url,sessions,new_users,bounce_rate_pct,avg_session_sec,goal_completions) VALUES (?,?,?,?,?,?,?)", [
        ('2023-01-01','/products/marketing-automation',820,610,48.2,142,12),
        ('2023-04-01','/products/marketing-automation',1240,880,44.1,158,22),
        ('2023-07-01','/products/marketing-automation',1890,1320,40.5,172,38),
        ('2023-10-01','/products/marketing-automation',2400,1640,38.2,185,52),
        ('2024-01-01','/products/marketing-automation',3100,2080,35.8,198,71),
        ('2024-04-01','/products/marketing-automation',3800,2480,33.2,212,96),
        ('2024-07-01','/products/marketing-automation',4500,2880,30.9,224,122),
        ('2024-10-01','/products/marketing-automation',5200,3240,28.4,238,148),
        ('2023-01-01','/blog/what-is-marketing-automation',1240,1080,62.4,95,8),
        ('2023-04-01','/blog/what-is-marketing-automation',1980,1720,59.1,108,14),
        ('2023-07-01','/blog/what-is-marketing-automation',2840,2380,55.8,122,24),
        ('2023-10-01','/blog/what-is-marketing-automation',3600,2980,52.4,138,35),
        ('2024-01-01','/blog/what-is-marketing-automation',4400,3620,48.2,155,48),
        ('2024-04-01','/blog/what-is-marketing-automation',5800,4740,44.8,172,68),
        ('2024-07-01','/blog/what-is-marketing-automation',7200,5840,41.4,188,89),
        ('2024-10-01','/blog/what-is-marketing-automation',9100,7320,38.0,205,116),
        ('2023-04-01','/blog/content-marketing-strategy',680,580,64.2,88,4),
        ('2023-07-01','/blog/content-marketing-strategy',1420,1200,60.8,102,10),
        ('2023-10-01','/blog/content-marketing-strategy',2240,1880,57.4,118,18),
        ('2024-01-01','/blog/content-marketing-strategy',3100,2580,54.0,134,28),
        ('2024-04-01','/blog/content-marketing-strategy',4200,3460,50.2,148,42),
        ('2024-07-01','/blog/content-marketing-strategy',5600,4580,46.8,162,60),
        ('2024-10-01','/blog/content-marketing-strategy',7200,5840,43.4,178,80),
        ('2023-07-01','/blog/digital-marketing-trends',820,720,66.4,82,3),
        ('2023-10-01','/blog/digital-marketing-trends',2100,1820,62.0,96,9),
        ('2024-01-01','/blog/digital-marketing-trends',3400,2940,58.4,112,18),
        ('2024-04-01','/blog/digital-marketing-trends',5100,4380,54.8,126,32),
        ('2024-07-01','/blog/digital-marketing-trends',7400,6300,51.2,140,52),
        ('2024-10-01','/blog/digital-marketing-trends',9800,8260,47.6,154,76),
        ('2024-01-01','/pricing',2200,1480,42.0,188,38),
        ('2024-04-01','/pricing',3400,2240,38.4,202,62),
        ('2024-10-01','/pricing',5100,3300,34.8,218,98),
    ])

    # ── GTM TAGS ──────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO gtm_tags (tag_name,tag_type,trigger_type,trigger_detail,is_active) VALUES (?,?,?,?,?)", [
        ('GA4 Configuration','GA4 Event','Page View','All Pages',1),
        ('GA4 Page View','GA4 Event','Page View','All Pages',1),
        ('GA4 Scroll Tracking','GA4 Event','Scroll Depth','Scroll Depth 25/50/75/90',1),
        ('GA4 Outbound Clicks','GA4 Event','Click','All Outbound Links',1),
        ('GA4 Form Submit','GA4 Event','Form Submit','All Forms',1),
        ('GA4 Demo Request','GA4 Event','Form Submit','Demo Form Success',1),
        ('GA4 Free Trial Signup','GA4 Event','Form Submit','Trial Form Success',1),
        ('GA4 Pricing Page View','GA4 Event','Page View','Pricing Page',1),
        ('Google Ads Conversion - Demo','Google Ads Conversion','Form Submit','Demo Form Success',1),
        ('Google Ads Conversion - Trial','Google Ads Conversion','Form Submit','Trial Form Success',1),
        ('Google Ads Remarketing','Google Ads Conversion','Page View','All Pages',1),
        ('Meta Pixel Base','Meta Pixel','Page View','All Pages',1),
        ('Meta Lead Event','Meta Pixel','Form Submit','Lead Form Success',1),
        ('Meta ViewContent','Meta Pixel','Page View','Product Pages',1),
        ('LinkedIn Insight Tag','LinkedIn','Page View','All Pages',1),
        ('LinkedIn Lead Gen','LinkedIn','Form Submit','Thank You Page',1),
        ('HotJar Heatmap','Custom HTML','Page View','All Pages excl Pricing',0),
        ('Intercom Live Chat','Custom HTML','Page View','All Pages',1),
        ('Cookie Consent Banner','Custom HTML','Custom Event','DOM Ready',1),
        ('Custom Event - Video Play','GA4 Event','Click','Video Element Click',0),
    ])

    # ── AD GROUPS ─────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO ad_groups (campaign_id,ad_group_name,bid_strategy,max_cpc,status) VALUES (?,?,?,?,?)", [
        (2,'Enterprise - Brand','target_cpa',12.00,'active'),
        (2,'Enterprise - Competitor','target_cpa',18.00,'active'),
        (2,'Enterprise - Generic','target_cpa',8.00,'active'),
        (4,'Spring Promo - High Intent','maximize_conversions',6.00,'paused'),
        (4,'Spring Promo - Brand','maximize_conversions',4.00,'paused'),
        (8,'Retargeting - Pricing Page','target_roas',15.00,'paused'),
        (8,'Retargeting - Blog Readers','target_roas',8.00,'paused'),
        (10,'Holiday - Cart Abandonment','target_roas',20.00,'paused'),
        (10,'Holiday - Pricing Visitors','target_roas',16.00,'paused'),
        (13,'ABM - Tier 1 Accounts','target_cpa',22.00,'active'),
        (13,'ABM - Tier 2 Accounts','target_cpa',16.00,'active'),
        (17,'Scale - High Intent KWs','target_cpa',14.00,'active'),
        (17,'Scale - Mid Funnel','target_cpa',9.00,'active'),
        (22,'Expansion - New Verticals','maximize_conversions',10.00,'active'),
    ])

    # ── ADS ───────────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO ads (ad_group_id,headline_1,headline_2,headline_3,description_1,description_2,final_url,ad_type,status) VALUES (?,?,?,?,?,?,?,?,?)", [
        (1,'Marketing Automation Platform','Automate Your Marketing','Start Free Trial Today','Save 12 hours/week with intelligent automation.','Connect your entire marketing stack in minutes.','/products/marketing-automation','rsa','active'),
        (2,'Better Than HubSpot','Save 40% on Your Plan','See the Comparison','Faster setup, better support, lower cost.','Join 5000+ teams that switched from HubSpot.','/compare/hubspot','rsa','active'),
        (3,'Marketing Automation Software','Scale Your Campaigns','Book a Free Demo','Enterprise-grade marketing platform.','Advanced analytics and AI-powered insights.','/demo','rsa','active'),
        (4,'Spring Sale: 30% Off','Marketing Automation Deal','Limited Time Offer','Our best price of the year. Ends April 30th.',None,'/pricing','rsa','paused'),
        (5,'Marketing Platform on Sale','Start Automating Today','See Spring Pricing','Powerful automation at a price that makes sense.',None,'/pricing','rsa','paused'),
        (6,'Still Comparing Platforms?','See Why We Win','Lock In Your Rate','You visited pricing. Questions? Talk to us now.',None,'/demo','rsa','paused'),
        (7,'Automate What You Just Read','See It Live in 20 Min','Free Demo Available','Ready to stop reading and start automating?',None,'/demo','rsa','paused'),
        (8,'Year-End Special Pricing','Don t Miss This Deal','Save Before Jan 1st','Our best pricing of the year. Expires Dec 31st.',None,'/pricing','rsa','paused'),
        (9,'Close 2023 Strong','Lock In Annual Rate','Act Before Year-End',None,'Start 2024 with your team fully onboarded.','/demo','rsa','paused'),
        (10,'Built for Enterprise Teams','Dedicated Onboarding','Talk to Enterprise Sales','Custom pricing and white-glove implementation.','Your success manager starts on day one.','/enterprise','rsa','active'),
        (11,'Scale Your Marketing Ops','All-in-One Platform','See Features','The platform that scales with your marketing team.',None,'/products','rsa','active'),
        (12,'Switch From HubSpot','Save 40% Annually','See Comparison','Marketing teams save an average of 18k per year.',None,'/compare/hubspot','rsa','active'),
        (13,'Year End Pricing','Save Before 2025','Book Demo Now','Our best pricing of the year. Ends December 31st.',None,'/pricing','rsa','active'),
        (14,'Close 2024 Strong','Lock In Annual Price','Act Now',None,'Start 2025 with your team fully onboarded.','/demo','rsa','active'),
    ])

    # ── AD PERFORMANCE ────────────────────────────────────────────────────────
    c.executemany("INSERT INTO ad_performance (ad_id,perf_date,impressions,clicks,spend,conversions,conversion_value,quality_score) VALUES (?,?,?,?,?,?,?,?)", [
        (1,'2023-01-01',18200,728,3276,8,96000,7),(1,'2023-04-01',22400,1008,4032,11,132000,7),
        (1,'2023-07-01',26800,1340,5360,14,168000,8),(1,'2023-10-01',31200,1872,6240,18,216000,8),
        (1,'2024-01-01',34800,2088,6264,22,264000,8),(1,'2024-04-01',38200,2674,6704,28,336000,9),
        (1,'2024-07-01',41600,3120,7488,34,408000,9),(1,'2024-10-01',46200,3696,8554,42,504000,9),
        (4,'2023-07-01',14200,710,4260,6,90000,6),(4,'2023-10-01',18400,1104,5888,9,135000,6),
        (4,'2024-01-01',21200,1484,7420,11,165000,7),(4,'2024-04-01',24800,1984,9424,14,210000,7),
        (4,'2024-07-01',28400,2556,12141,18,270000,8),(4,'2024-10-01',32600,3260,15511,22,330000,8),
        (7,'2023-10-01',28400,1988,9940,16,320000,7),(7,'2024-10-01',34200,2736,12312,24,480000,8),
        (9,'2024-01-01',32400,1944,7776,18,252000,8),(9,'2024-04-01',36800,2576,9030,22,308000,8),
        (9,'2024-07-01',42200,3376,12154,28,392000,9),(9,'2024-10-01',48600,4374,15309,36,504000,9),
    ])

    # ── EMAIL CAMPAIGNS ───────────────────────────────────────────────────────
    c.executemany("INSERT INTO email_campaigns (campaign_id,email_name,subject_line,preview_text,sender_name,audience_segment,email_type,list_size,send_date,status) VALUES (?,?,?,?,?,?,?,?,?,?)", [
        (3,'Q1 Welcome Nurture 1','Welcome to the platform','Everything you need to hit the ground running','Marketing Team','New SMB Customers','nurture',3200,'2023-02-08','sent'),
        (3,'Q1 Feature Spotlight','You asked for it: top 3 features','Our most-requested features explained','Marketing Team','SMB Active Users','newsletter',3800,'2023-02-22','sent'),
        (7,'Summer Value Send','Mid-year check-in: are you hitting your goals?','Quick tips to close Q3 strong','Marketing Team','SMB Customers','promotional',4200,'2023-07-12','sent'),
        (11,'Q4 Retention Series 1','Your renewal is coming up','See everything we shipped','Marketing Team','Enterprise Renewals','nurture',2800,'2023-10-18','sent'),
        (11,'Q4 Retention Series 2','Final reminder: renew before year-end','Lock in your rate before January 1st','Customer Success','Enterprise Renewals','nurture',2800,'2023-11-15','sent'),
        (12,'Referral Partner Launch','Earn up to 25% commission','Our referral program is live','Partnerships','Partner Contacts','promotional',1200,'2023-11-08','sent'),
        (14,'Q1 Webinar Invite','Exclusive webinar: Mastering B2B Marketing','Register now, seats are limited','Marketing Team','SMB Prospects','promotional',5600,'2024-01-24','sent'),
        (14,'Webinar Follow-Up','Your recording is inside','Watch any session you missed','Marketing Team','Webinar Attendees','nurture',2400,'2024-02-08','sent'),
        (15,'New Blog Digest','The 5 posts you need to read this month','We published a lot. Highlights inside.','Content Team','Blog Subscribers','newsletter',8200,'2024-02-20','sent'),
        (13,'Enterprise Intro','A message from our Enterprise team','Why 200+ enterprise teams chose us','Enterprise Sales','Enterprise Prospects','nurture',1800,'2024-02-05','sent'),
        (20,'Re-engagement Campaign','We miss you','A lot has changed. Come see for yourself.','Marketing Team','Inactive Users 90 Day','win_back',3400,'2024-04-10','sent'),
        (20,'Re-engagement Follow-up','One last thing before we say goodbye','A special offer for returning customers','Marketing Team','Inactive Users No Open','win_back',1800,'2024-04-24','sent'),
        (18,'Partner Program Q2 Update','Your Q2 commissions','Log in to see your earnings','Partnerships','Active Partners','newsletter',620,'2024-05-08','sent'),
        (24,'Enterprise Nurture: ROI Guide','The enterprise marketing ROI guide','How our top customers measure success','Marketing Team','Enterprise Prospects','nurture',2200,'2024-07-10','sent'),
        (24,'Enterprise Case Study Send','How Acme Corp 3x their pipeline','A detailed breakdown of their strategy','Marketing Team','Enterprise Qualified','nurture',1400,'2024-08-07','sent'),
        (21,'SEO Trends Newsletter','8 SEO changes you need to know','Straight from our content team','Content Team','Blog Subscribers','newsletter',9400,'2024-07-22','sent'),
        (26,'Holiday Campaign Launch','Our biggest promotion of the year','40% off annual plans. Ends Dec 31st.','Marketing Team','All Customers','promotional',14200,'2024-10-22','sent'),
        (26,'Holiday Reminder','Only 2 weeks left','Prices go back to normal Jan 1st','Marketing Team','All Prospects','promotional',22000,'2024-12-08','sent'),
        (27,'Year-End Referral Push','Refer a friend and earn 500 in credit','The easiest money you will make this quarter','Marketing Team','Active Customers','promotional',8600,'2024-11-12','sent'),
        (28,'Q4 Display Retargeting Email','You have been looking. Ready to talk?','A quick message from your account team','Sales Team','Pricing Page Visitors','nurture',3800,'2024-10-28','sent'),
    ])

    # ── EMAIL EVENTS ──────────────────────────────────────────────────────────
    c.executemany("INSERT INTO email_events (email_campaign_id,customer_id,event_type,event_at) VALUES (?,?,?,?)", [
        (1,1,'opened','2023-02-08 10:24:00'),(1,1,'clicked','2023-02-08 10:26:00'),
        (1,3,'opened','2023-02-08 11:05:00'),(1,5,'opened','2023-02-09 09:14:00'),
        (1,5,'clicked','2023-02-09 09:16:00'),(1,7,'opened','2023-02-09 14:30:00'),
        (1,9,'opened','2023-02-10 08:44:00'),(1,9,'unsubscribed','2023-02-10 08:46:00'),
        (2,1,'opened','2023-02-22 09:32:00'),(2,1,'clicked','2023-02-22 09:35:00'),
        (2,1,'converted','2023-02-22 09:48:00'),(2,3,'opened','2023-02-22 10:18:00'),
        (2,5,'opened','2023-02-23 08:55:00'),(2,5,'clicked','2023-02-23 09:02:00'),
        (3,3,'opened','2023-07-12 10:08:00'),(3,3,'clicked','2023-07-12 10:11:00'),
        (3,5,'opened','2023-07-12 11:22:00'),(3,7,'opened','2023-07-13 08:40:00'),
        (3,7,'clicked','2023-07-13 08:44:00'),(3,7,'converted','2023-07-13 09:10:00'),
        (4,1,'opened','2023-10-18 09:15:00'),(4,1,'clicked','2023-10-18 09:18:00'),
        (4,9,'opened','2023-10-18 10:30:00'),(4,11,'opened','2023-10-19 08:22:00'),
        (4,11,'clicked','2023-10-19 08:25:00'),(4,11,'converted','2023-10-19 08:45:00'),
        (5,1,'opened','2023-11-15 09:05:00'),(5,9,'opened','2023-11-15 10:14:00'),
        (7,12,'opened','2024-01-24 09:28:00'),(7,12,'clicked','2024-01-24 09:31:00'),
        (7,12,'converted','2024-01-24 09:42:00'),(7,14,'opened','2024-01-24 10:15:00'),
        (7,14,'clicked','2024-01-24 10:19:00'),(7,16,'opened','2024-01-25 08:50:00'),
        (8,12,'opened','2024-02-08 09:10:00'),(8,12,'clicked','2024-02-08 09:13:00'),
        (11,15,'opened','2024-04-10 09:30:00'),(11,15,'clicked','2024-04-10 09:33:00'),
        (11,15,'converted','2024-04-10 10:05:00'),(11,17,'opened','2024-04-11 10:20:00'),
        (14,19,'opened','2024-07-10 09:18:00'),(14,19,'clicked','2024-07-10 09:22:00'),
        (14,21,'opened','2024-07-10 10:40:00'),(14,21,'clicked','2024-07-10 10:44:00'),
        (14,21,'converted','2024-07-10 11:00:00'),(14,23,'opened','2024-07-11 08:28:00'),
        (17,1,'opened','2024-10-22 09:22:00'),(17,1,'clicked','2024-10-22 09:25:00'),
        (17,3,'opened','2024-10-22 10:15:00'),(17,3,'clicked','2024-10-22 10:19:00'),
        (17,3,'converted','2024-10-22 10:42:00'),(17,5,'opened','2024-10-23 08:30:00'),
        (17,7,'opened','2024-10-23 09:18:00'),(17,7,'clicked','2024-10-23 09:22:00'),
    ])

    # ── WEB EVENTS ────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO web_events (session_id,customer_id,tag_id,page_url,event_name,event_category,device_type,traffic_source,created_at) VALUES (?,?,?,?,?,?,?,?,?)", [
        ('sess_2023_001',1,1,'/products/marketing-automation','page_view','organic','desktop','google_organic','2023-01-20'),
        ('sess_2023_002',3,1,'/blog/what-is-marketing-automation','page_view','organic','desktop','google_organic','2023-02-05'),
        ('sess_2023_004',7,6,'/demo','form_submit','conversion','desktop','linkedin','2023-01-22'),
        ('sess_2023_005',9,6,'/demo','form_submit','conversion','desktop','google_paid','2023-02-15'),
        ('sess_2023_007',15,7,'/trial','form_submit','conversion','desktop','google_organic','2023-03-05'),
        ('sess_2024_001',25,15,'/enterprise','page_view','social','desktop','linkedin','2024-01-15'),
        ('sess_2024_003',31,6,'/demo','form_submit','conversion','desktop','google_paid','2024-01-20'),
        ('sess_2024_005',35,6,'/demo','form_submit','conversion','desktop','google_paid','2024-04-12'),
        ('sess_2024_007',39,8,'/pricing','page_view','paid','desktop','google_paid','2024-07-22'),
        ('sess_2024_009',43,9,'/thank-you','form_submit','conversion','desktop','google_paid','2024-08-12'),
        ('sess_2024_011',1,9,'/thank-you','form_submit','conversion','desktop','google_paid','2024-11-10'),
        ('sess_2024_012',3,13,'/thank-you','form_submit','conversion','mobile','facebook','2024-10-25'),
        ('sess_2024_014',9,1,'/products/marketing-automation','page_view','direct','desktop','direct','2024-09-14'),
        ('sess_2024_015',11,1,'/pricing','page_view','direct','desktop','direct','2024-09-18'),
    ])

    # ── WEB SESSIONS ──────────────────────────────────────────────────────────
    c.executemany("INSERT INTO web_sessions (session_id,customer_id,landing_page,referrer_source,referrer_medium,utm_campaign,device_type,session_start,session_end,pages_viewed,converted) VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
        ('sess_2023_001',1,'/products/marketing-automation','google_organic','organic',None,'desktop','2023-01-20 09:14:00','2023-01-20 09:28:00',4,0),
        ('sess_2023_002',3,'/blog/what-is-marketing-automation','google_organic','organic',None,'desktop','2023-02-05 10:22:00','2023-02-05 10:38:00',6,0),
        ('sess_2023_003',5,'/products/marketing-automation','google_paid','cpc','q1_23_enterprise','desktop','2023-02-15 09:05:00','2023-02-15 09:42:00',8,1),
        ('sess_2023_004',7,'/enterprise','linkedin','cpc','q1_23_enterprise','desktop','2023-02-22 11:00:00','2023-02-22 11:18:00',5,1),
        ('sess_2023_005',9,'/pricing','google_organic','organic',None,'desktop','2023-03-10 14:18:00','2023-03-10 14:45:00',7,1),
        ('sess_2023_006',11,'/products/marketing-automation','google_paid','cpc','q2_23_spring_paid','mobile','2023-04-08 09:30:00','2023-04-08 09:52:00',5,1),
        ('sess_2023_007',12,'/blog/what-is-marketing-automation','facebook','social','q2_23_social','mobile','2023-05-12 10:14:00','2023-05-12 10:30:00',3,0),
        ('sess_2023_008',13,'/pricing','google_paid','cpc','q3_23_retargeting','desktop','2023-07-18 09:20:00','2023-07-18 09:38:00',4,1),
        ('sess_2023_009',15,'/products/marketing-automation','direct',None,None,'desktop','2023-07-22 11:05:00','2023-07-22 11:28:00',6,0),
        ('sess_2023_010',17,'/pricing','google_paid','cpc','q4_23_holiday','desktop','2023-10-08 09:44:00','2023-10-08 10:12:00',8,1),
        ('sess_2024_001',19,'/enterprise','linkedin','cpc','q1_24_abm','desktop','2024-01-15 09:28:00','2024-01-15 09:56:00',7,1),
        ('sess_2024_002',21,'/enterprise','linkedin','cpc','q1_24_abm','desktop','2024-02-08 10:44:00','2024-02-08 11:10:00',9,1),
        ('sess_2024_003',23,'/products/marketing-automation','google_paid','cpc','q2_24_paid_search','desktop','2024-04-08 09:30:00','2024-04-08 09:58:00',6,1),
        ('sess_2024_004',25,'/enterprise','linkedin','cpc','q2_24_linkedin_abm','mobile','2024-04-22 10:18:00','2024-04-22 10:42:00',5,0),
        ('sess_2024_005',27,'/products/marketing-automation','referral',None,None,'desktop','2024-05-15 11:00:00','2024-05-15 11:24:00',4,1),
        ('sess_2024_006',29,'/enterprise','google_paid','cpc','q3_24_expansion','desktop','2024-07-10 09:18:00','2024-07-10 09:48:00',8,1),
        ('sess_2024_007',31,'/blog/what-is-marketing-automation','google_organic','organic',None,'mobile','2024-07-22 14:10:00','2024-07-22 14:38:00',6,0),
        ('sess_2024_008',33,'/enterprise','linkedin','cpc','q3_24_expansion','desktop','2024-08-05 10:28:00','2024-08-05 10:58:00',7,1),
        ('sess_2024_009',35,'/pricing','google_paid','cpc','q4_24_holiday','desktop','2024-10-08 09:52:00','2024-10-08 10:22:00',9,1),
        ('sess_2024_010',37,'/products/marketing-automation','email','email','q4_24_holiday_email','desktop','2024-10-22 09:22:00','2024-10-22 09:48:00',5,1),
        ('sess_2024_011',39,'/pricing','facebook','paid','q4_24_holiday','mobile','2024-10-25 10:18:00','2024-10-25 10:44:00',6,1),
        ('sess_2024_012',41,'/products/marketing-automation','facebook','paid','q4_24_holiday','mobile','2024-11-05 11:42:00','2024-11-05 12:08:00',7,1),
        ('sess_2024_013',43,'/enterprise','linkedin','cpc','q4_24_linkedin','desktop','2024-11-08 09:05:00','2024-11-08 09:35:00',4,1),
        ('sess_2024_014',45,'/products/marketing-automation','referral',None,None,'desktop','2024-11-12 09:18:00','2024-11-12 09:48:00',5,1),
        ('sess_2024_015',1,'/blog/what-is-marketing-automation','direct',None,None,'desktop','2024-11-18 14:30:00','2024-11-18 15:00:00',8,0),
    ])

    # ── CONTENT PIECES ────────────────────────────────────────────────────────
    c.executemany("INSERT INTO content_pieces (title,content_type,topic_cluster,target_keyword,author,word_count,publish_date,status,cta_type,campaign_id) VALUES (?,?,?,?,?,?,?,?,?,?)", [
        ('The Ultimate Guide to Marketing Automation','blog_post','Marketing Automation','marketing automation','Content Team',4200,'2023-01-15','published','demo_request',1),
        ('5 Ways Email Automation Saves 12 Hours Per Week','blog_post','Email Marketing','email automation','Content Team',1800,'2023-02-01','published','demo_request',3),
        ('Marketing Automation ROI: The 2023 Benchmark Report','whitepaper','Marketing Automation','marketing automation ROI','Research Team',5800,'2023-03-01','published','download',None),
        ('HubSpot vs. Our Platform: Side-by-Side Comparison','landing_page','Competitive','HubSpot alternative','Content Team',2400,'2023-03-15','published','demo_request',4),
        ('How to Build a Marketing Attribution Model','blog_post','Analytics','marketing attribution','Content Team',3600,'2023-04-01','published','demo_request',None),
        ('The Complete Guide to Lead Scoring','blog_post','Lead Management','lead scoring','Content Team',3200,'2023-04-15','published','demo_request',None),
        ('B2B Email Marketing: 2023 Strategy Guide','blog_post','Email Marketing','B2B email marketing','Content Team',2800,'2023-05-01','published','demo_request',7),
        ('Customer Journey Mapping for B2B Marketers','blog_post','Funnel Analytics','customer journey mapping','Content Team',3000,'2023-05-15','published','contact',None),
        ('Case Study: How Acme Corp 3x Their Pipeline','case_study','Social Proof','marketing automation case study','Content Team',2200,'2023-06-01','published','contact',None),
        ('Marketing Analytics Dashboard Examples','blog_post','Analytics','marketing dashboard examples','Content Team',1600,'2023-07-01','published','demo_request',None),
        ('The 2023 State of Marketing Automation Report','whitepaper','Marketing Automation','marketing automation trends','Research Team',6200,'2023-08-01','published','download',None),
        ('SEO for B2B Marketers: The Complete 2023 Guide','blog_post','SEO Analytics','B2B SEO','Content Team',4400,'2023-09-01','published','demo_request',None),
        ('10 Marketing Automation Workflows That Actually Work','blog_post','Marketing Automation','marketing automation workflow','Content Team',2000,'2023-10-01','published','demo_request',10),
        ('Multi-Touch Attribution: A Practical Guide','blog_post','Analytics','multi-touch attribution marketing','Content Team',3800,'2023-11-01','published','demo_request',None),
        ('Year-End Marketing Planning Template','infographic','Funnel Analytics','marketing plan template','Content Team',1200,'2023-12-01','published','download',11),
        ('Marketing Automation Pricing: What to Expect in 2024','blog_post','Marketing Automation','marketing automation pricing','Content Team',2200,'2024-01-15','published','contact',13),
        ('Enterprise Marketing Automation: The Buyer Guide','whitepaper','Marketing Automation','enterprise marketing automation','Content Team',5200,'2024-02-01','published','contact',13),
        ('The B2B Content Marketing Strategy Playbook','blog_post','Content Marketing','content marketing strategy','Content Team',4400,'2024-02-15','published','download',15),
        ('CRM Integration Guide: Connecting Your Marketing Stack','blog_post','CRM Integration','CRM marketing integration','Content Team',3600,'2024-03-01','published','demo_request',None),
        ('Digital Marketing Trends 2024: What to Watch','blog_post','Trends','digital marketing trends','Content Team',3600,'2024-04-01','published','subscribe',20),
        ('LinkedIn ABM Playbook for B2B Teams','blog_post','Social Media','LinkedIn marketing','Content Team',3200,'2024-04-15','published','demo_request',19),
        ('Marketing Attribution in a Cookieless World','blog_post','Analytics','marketing attribution','Content Team',4000,'2024-05-01','published','demo_request',None),
        ('How to Calculate Marketing ROI (With Examples)','blog_post','Analytics','marketing ROI','Content Team',2400,'2024-06-01','published','contact',None),
        ('The Definitive Guide to B2B Lead Generation 2024','blog_post','Lead Management','b2b lead generation tactics','Content Team',5400,'2024-07-01','published','demo_request',22),
        ('SEO Trends 2024: What Marketers Need to Know','blog_post','SEO Analytics','digital marketing trends','Content Team',2800,'2024-07-15','published','demo_request',21),
        ('Case Study: Enterprise Customer 5x Revenue Attribution','case_study','Social Proof','marketing automation case study','Content Team',2600,'2024-08-01','published','contact',24),
        ('Email Marketing Benchmarks 2024','whitepaper','Email Marketing','email marketing','Research Team',4800,'2024-09-01','published','download',None),
        ('Year-End Marketing Planning Guide 2025','blog_post','Funnel Analytics','marketing plan template','Content Team',3800,'2024-10-01','published','download',26),
        ('Black Friday Campaign Strategy for B2B Teams','blog_post','Marketing Automation','b2b marketing campaigns','Content Team',2200,'2024-10-15','published','demo_request',25),
        ('2025 Marketing Predictions: AI, Privacy, and Performance','blog_post','Trends','digital marketing trends 2024','Content Team',3600,'2024-11-01','published','subscribe',None),
    ])

    # ── CONTENT PERFORMANCE ───────────────────────────────────────────────────
    c.executemany("INSERT INTO content_performance (content_id,perf_date,page_views,unique_visitors,avg_time_sec,bounce_rate_pct,social_shares,comments,backlinks_earned,cta_clicks,conversions) VALUES (?,?,?,?,?,?,?,?,?,?,?)", [
        (1,'2023-02-01',2840,2420,142,58.2,124,18,8,98,12),(1,'2023-05-01',4200,3580,168,54.4,186,24,14,154,22),
        (1,'2023-08-01',6100,5060,188,50.8,242,31,22,228,34),(1,'2023-11-01',8400,6920,204,47.2,314,42,31,308,48),
        (1,'2024-02-01',10800,8840,218,44.2,388,52,42,402,64),(1,'2024-05-01',13400,10840,232,41.4,462,64,54,496,82),
        (1,'2024-08-01',16200,13000,244,38.8,538,76,68,590,102),(1,'2024-11-01',19400,15440,256,36.4,614,89,84,684,124),
        (5,'2023-04-01',1820,1560,198,52.4,88,12,6,64,8),(5,'2023-07-01',3200,2720,224,48.8,148,20,12,112,14),
        (5,'2023-10-01',4800,4040,244,45.4,214,28,18,168,22),(5,'2024-01-01',6400,5360,264,42.2,282,38,26,224,32),
        (5,'2024-04-01',8200,6840,282,39.4,352,48,36,288,44),(5,'2024-07-01',10200,8440,298,36.8,428,60,48,352,58),
        (5,'2024-10-01',12400,10200,314,34.4,504,74,62,420,74),
        (20,'2024-02-01',4200,3640,168,54.8,198,28,12,148,18),(20,'2024-05-01',7800,6680,188,50.4,368,48,24,284,36),
        (20,'2024-08-01',12400,10480,208,46.2,568,72,38,448,58),(20,'2024-11-01',18200,15280,226,42.4,784,98,56,624,84),
    ])

    # ── AUDIENCES ─────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO audiences (audience_name,channel,audience_type,criteria_description,size_estimate,match_rate_pct,is_active) VALUES (?,?,?,?,?,?,?)", [
        ('Enterprise Decision Makers','LinkedIn','demographic','Job title: VP, Director, C-Suite. Company size: 500+.',48000,None,1),
        ('SMB Marketing Managers','LinkedIn','demographic','Job title: Marketing Manager. Company size: 10-500.',124000,None,1),
        ('All Website Visitors 30 Day','Google','remarketing','Visited any page on our site in last 30 days',18400,None,1),
        ('Pricing Page Visitors','Google','remarketing','Visited /pricing in last 14 days. Did not convert.',2800,None,1),
        ('Demo Page Non-Converters','Google','remarketing','Visited /demo but did not submit form.',1600,None,1),
        ('Blog Readers - High Engagement','Google','remarketing','Visited 3+ blog posts in 30 days',4200,None,1),
        ('Lookalike - Converted Customers','Meta','lookalike','1% lookalike of our converted customer list.',220000,62.4,1),
        ('Interest - Marketing Technology','Meta','interest','Interests: marketing technology, CRM, email marketing',580000,None,1),
        ('Retargeting - Site Visitors 60 Day','Meta','remarketing','Visited site in last 60 days. Excludes converted.',24600,None,1),
        ('Email Subscribers - Active','Email','custom','Opened at least 1 email in last 90 days',8400,None,1),
        ('Email - High Engagement Segment','Email','custom','Clicked in 2+ campaigns in last 60 days',2800,None,1),
        ('CRM - Qualified Leads','Email','custom','Lead status = qualified. Lead score >= 65',1200,None,1),
        ('LinkedIn - Matched Company List','LinkedIn','custom','Matched to target account list. 500 ICP companies.',38000,48.2,1),
        ('Google - Customer Match','Google','custom','Customer email list. Converted customers only.',42000,71.8,1),
        ('Meta - Lookalike SMB Customers','Meta','lookalike','2% lookalike of SMB customer segment',640000,58.6,1),
    ])

    # ── AUDIENCE MEMBERS ──────────────────────────────────────────────────────
    c.executemany("INSERT INTO audience_members (audience_id,customer_id,added_at) VALUES (?,?,?)", [
        (1,1,'2023-01-20'),(1,9,'2023-01-20'),(1,19,'2023-01-20'),
        (2,4,'2023-02-01'),(2,14,'2023-02-01'),(2,24,'2023-02-01'),
        (3,5,'2023-03-01'),(3,15,'2023-03-01'),(3,25,'2023-03-01'),
        (4,7,'2023-04-15'),(4,17,'2023-04-15'),
        (5,3,'2023-07-01'),(5,13,'2023-07-01'),
        (6,9,'2024-01-01'),(6,21,'2024-01-01'),(6,31,'2024-01-01'),
        (10,1,'2024-04-01'),(10,3,'2024-04-01'),
        (14,15,'2024-07-01'),
    ])

    # ── AB TESTS ──────────────────────────────────────────────────────────────
    c.executemany("INSERT INTO ab_tests (test_name,test_type,campaign_id,content_id,email_campaign_id,hypothesis,start_date,end_date,status,winner_variant,confidence_pct,primary_metric) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        ('Email Subject Line Test Q1 2023','subject_line',3,1,1,'Personalized subject lines will increase open rate by 15%','2023-02-08','2023-03-08','completed','B',95.0,'open_rate'),
        ('Landing Page CTA Color Test','landing_page',2,None,None,'Red CTA button will improve conversion rate over green','2023-02-15','2023-03-15','completed','A',92.0,'conversion_rate'),
        ('Webinar Registration Form Test','landing_page',3,None,2,'Short form will improve signups vs long form','2023-02-20','2023-03-20','completed','B',97.0,'conversion_rate'),
        ('Retargeting Ad Copy Test Q3','ad_copy',8,None,None,'Question headline will outperform statement in CTR','2023-07-15','2023-08-15','completed','B',91.0,'ctr'),
        ('Email Personalization Test Q4','subject_line',11,None,4,'Dynamic content blocks will increase click rate','2023-10-18','2023-11-18','completed','A',88.0,'click_rate'),
        ('Homepage Hero Image Test','landing_page',None,1,None,'Product screenshot hero will outperform abstract graphic','2023-11-01','2023-12-01','completed','A',94.0,'conversion_rate'),
        ('Demo Request Form Length Test','landing_page',13,None,None,'Reducing form fields from 6 to 3 will increase demos','2024-01-10','2024-02-10','completed','B',96.0,'conversion_rate'),
        ('Enterprise Nurture Email Cadence Test','subject_line',13,None,10,'Weekly emails will outperform biweekly in pipeline','2024-02-05','2024-03-05','completed','A',89.0,'pipeline_created'),
        ('Q2 Paid Search Ad Copy Test','ad_copy',17,None,None,'Feature-led headline will beat benefit-led in conversion','2024-04-01','2024-05-01','completed','B',93.0,'conversion_rate'),
        ('Content Upgrade CTA Test','cta',20,20,11,'Inline CTA in body will outperform sidebar CTA','2024-04-10','2024-05-10','completed','A',90.0,'conversion_rate'),
        ('Holiday Email Subject Test Q4','subject_line',26,None,17,'Urgency subject line will beat curiosity subject in opens','2024-10-22','2024-11-22','completed','B',97.0,'open_rate'),
        ('Pricing Page Layout Test Q4','landing_page',25,29,None,'Annual-first pricing table will improve annual plan selection','2024-10-15','2024-11-15','completed','A',92.0,'conversion_rate'),
    ])

    # ── AB VARIANTS ───────────────────────────────────────────────────────────
    c.executemany("INSERT INTO ab_variants (test_id,variant_name,variant_detail,impressions,conversions,revenue) VALUES (?,?,?,?,?,?)", [
        (1,'control','Standard subject line',1600,96,57600),(1,'variant_a','Personalized subject line',1600,136,81600),
        (2,'control','Green CTA button',8400,378,226800),(2,'variant_a','Red CTA button',8400,462,277200),
        (3,'control','Long form 7 fields',2400,216,129600),(3,'variant_a','Short form 3 fields',2400,312,187200),
        (4,'control','Statement headline',14200,568,340800),(4,'variant_a','Question headline',14200,710,426000),
        (5,'control','No personalization',1400,84,50400),(5,'variant_a','Dynamic content blocks',1400,112,67200),
        (6,'control','Abstract graphic hero',22400,1120,672000),(6,'variant_a','Product screenshot hero',22400,1568,940800),
        (7,'control','Long form 6 fields',4800,336,403200),(7,'variant_a','Short form 3 fields',4800,480,576000),
        (8,'control','Biweekly email cadence',900,72,432000),(8,'variant_a','Weekly email cadence',900,90,540000),
        (9,'control','Feature-led headline',19400,776,465600),(9,'variant_a','Benefit-led headline',19400,970,582000),
        (10,'control','Sidebar CTA',7200,360,216000),(10,'variant_a','Inline body CTA',7200,504,302400),
        (11,'control','Curiosity subject line',11000,770,462000),(11,'variant_a','Urgency subject line',11000,990,594000),
        (12,'control','Monthly plan first',6800,544,544000),(12,'variant_a','Annual plan first',6800,748,748000),
    ])

    conn.commit()
    conn.close()

    size = os.path.getsize(DB_PATH)
    print(f"\n✅ Database created: {DB_PATH}")
    print(f"   Size: {size/1024:.1f} KB")
    print(f"\nNext steps:")
    print(f"  1. Run: python monitor.py  (generates initial recommendations)")
    print(f"  2. Run: streamlit run app.py")

if __name__ == "__main__":
    create_db()
