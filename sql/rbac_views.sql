CREATE VIEW IF NOT EXISTS v_exec_kpi_daily AS
SELECT * FROM fact_kpi_daily;

CREATE VIEW IF NOT EXISTS v_ops_kpi_daily AS
SELECT *
FROM fact_kpi_daily
WHERE kpi_name IN (
    'on_time_delivery_pct',
    'sla_breach_rate_pct',
    'exception_rate_per_100_jobs',
    'manual_workload_hours_weekly',
    'data_quality_score',
    'framework_adoption_proxy_pct',
    'bi_utilisation_proxy_requests',
    'stakeholder_satisfaction_proxy_rating'
);

CREATE VIEW IF NOT EXISTS v_finance_kpi_daily AS
SELECT *
FROM fact_kpi_daily
WHERE kpi_name IN (
    'cost_leakage_estimate_gbp',
    'automation_impact_hours_weekly',
    'automation_impact_gbp_weekly',
    'automation_impact_gbp_cumulative',
    'data_quality_score',
    'framework_adoption_proxy_pct',
    'bi_utilisation_proxy_requests',
    'stakeholder_satisfaction_proxy_rating'
);
