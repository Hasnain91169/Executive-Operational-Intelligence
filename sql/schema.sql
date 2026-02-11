PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date TEXT NOT NULL UNIQUE,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_site (
    site_id INTEGER PRIMARY KEY,
    site_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    tier TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_carrier (
    carrier_id INTEGER PRIMARY KEY,
    carrier_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    product_family TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_jobs (
    job_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    carrier_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    value_gbp REAL NOT NULL,
    promised_date_key INTEGER NOT NULL,
    delivered_date_key INTEGER,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    duplicate_flag INTEGER NOT NULL DEFAULT 0,
    source_batch TEXT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (site_id) REFERENCES dim_site(site_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (team_id) REFERENCES dim_team(team_id),
    FOREIGN KEY (carrier_id) REFERENCES dim_carrier(carrier_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (promised_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (delivered_date_key) REFERENCES dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS fact_incidents (
    incident_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    job_id TEXT,
    incident_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    minutes_lost INTEGER NOT NULL,
    product_id INTEGER,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (site_id) REFERENCES dim_site(site_id),
    FOREIGN KEY (job_id) REFERENCES fact_jobs(job_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE TABLE IF NOT EXISTS fact_comms (
    comm_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    minutes_spent INTEGER NOT NULL,
    sla_sensitive_bool INTEGER NOT NULL,
    response_minutes INTEGER NOT NULL,
    breached_bool INTEGER NOT NULL,
    job_id TEXT,
    carrier_id INTEGER,
    product_id INTEGER,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (site_id) REFERENCES dim_site(site_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
    FOREIGN KEY (job_id) REFERENCES fact_jobs(job_id),
    FOREIGN KEY (carrier_id) REFERENCES dim_carrier(carrier_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE TABLE IF NOT EXISTS fact_costs (
    cost_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    cost_type TEXT NOT NULL,
    amount_gbp REAL NOT NULL,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (site_id) REFERENCES dim_site(site_id)
);

CREATE TABLE IF NOT EXISTS fact_automation_events (
    event_id TEXT PRIMARY KEY,
    date_key INTEGER NOT NULL,
    site_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    hours_saved REAL NOT NULL,
    gbp_saved REAL NOT NULL,
    notes TEXT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (site_id) REFERENCES dim_site(site_id)
);

CREATE TABLE IF NOT EXISTS kpi_definitions (
    kpi_name TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    formula_text TEXT NOT NULL,
    grain TEXT NOT NULL,
    owner_role TEXT,
    threshold_good REAL,
    threshold_bad REAL,
    refresh_cadence TEXT,
    business_value TEXT,
    leading_indicator_bool INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_kpi_daily (
    kpi_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    date_key INTEGER NOT NULL,
    kpi_name TEXT NOT NULL,
    value REAL NOT NULL,
    target_good REAL,
    target_bad REAL,
    owner_role TEXT,
    status TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    UNIQUE (date, kpi_name),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (kpi_name) REFERENCES kpi_definitions(kpi_name)
);

CREATE TABLE IF NOT EXISTS anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kpi_name TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    baseline REAL NOT NULL,
    score REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    scenario_tag TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scenario_registry (
    scenario_tag TEXT PRIMARY KEY,
    scenario_date TEXT NOT NULL,
    description TEXT NOT NULL,
    kpi_name TEXT,
    expected_driver_dimension TEXT,
    expected_driver_value TEXT
);

CREATE TABLE IF NOT EXISTS explain_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    kpi_name TEXT NOT NULL,
    date TEXT NOT NULL,
    sql_used TEXT NOT NULL,
    slices_returned TEXT NOT NULL,
    user_feedback TEXT,
    request_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_id INTEGER NOT NULL,
    rating INTEGER NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (audit_id) REFERENCES explain_audit_log(id)
);

CREATE TABLE IF NOT EXISTS automations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    trigger_kpi TEXT NOT NULL,
    condition_json TEXT NOT NULL,
    webhook_url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    FOREIGN KEY (trigger_kpi) REFERENCES kpi_definitions(kpi_name)
);

CREATE TABLE IF NOT EXISTS automation_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    automation_id INTEGER,
    name TEXT NOT NULL,
    kpi_name TEXT NOT NULL,
    date TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    response_code INTEGER,
    response_body TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (automation_id) REFERENCES automations(id)
);

CREATE TABLE IF NOT EXISTS api_usage_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,
    method TEXT NOT NULL,
    role TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    requested_at TEXT NOT NULL,
    request_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    check_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    score REAL NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS contract_validation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TEXT NOT NULL,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_fact_jobs_date_site ON fact_jobs(date_key, site_id);
CREATE INDEX IF NOT EXISTS idx_fact_comms_date_site ON fact_comms(date_key, site_id);
CREATE INDEX IF NOT EXISTS idx_fact_incidents_date_site ON fact_incidents(date_key, site_id);
CREATE INDEX IF NOT EXISTS idx_fact_kpi_daily_name_date ON fact_kpi_daily(kpi_name, date);
CREATE INDEX IF NOT EXISTS idx_anomalies_status ON anomalies(status);
CREATE INDEX IF NOT EXISTS idx_api_usage_endpoint_time ON api_usage_log(endpoint, requested_at);
