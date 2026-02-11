CREATE VIEW IF NOT EXISTS vw_kpi_latest AS
SELECT fk.*
FROM fact_kpi_daily fk
JOIN (
    SELECT kpi_name, MAX(date) AS max_date
    FROM fact_kpi_daily
    GROUP BY kpi_name
) latest
    ON latest.kpi_name = fk.kpi_name
   AND latest.max_date = fk.date;

CREATE VIEW IF NOT EXISTS vw_kpi_summary_by_day AS
SELECT
    date,
    SUM(CASE WHEN kpi_name = 'on_time_delivery_pct' THEN value ELSE 0 END) AS on_time_delivery_pct,
    SUM(CASE WHEN kpi_name = 'sla_breach_rate_pct' THEN value ELSE 0 END) AS sla_breach_rate_pct,
    SUM(CASE WHEN kpi_name = 'exception_rate_per_100_jobs' THEN value ELSE 0 END) AS exception_rate_per_100_jobs,
    SUM(CASE WHEN kpi_name = 'manual_workload_hours_weekly' THEN value ELSE 0 END) AS manual_workload_hours_weekly,
    SUM(CASE WHEN kpi_name = 'cost_leakage_estimate_gbp' THEN value ELSE 0 END) AS cost_leakage_estimate_gbp,
    SUM(CASE WHEN kpi_name = 'data_quality_score' THEN value ELSE 0 END) AS data_quality_score,
    SUM(CASE WHEN kpi_name = 'automation_impact_hours_weekly' THEN value ELSE 0 END) AS automation_impact_hours_weekly,
    SUM(CASE WHEN kpi_name = 'automation_impact_gbp_weekly' THEN value ELSE 0 END) AS automation_impact_gbp_weekly,
    SUM(CASE WHEN kpi_name = 'automation_impact_gbp_cumulative' THEN value ELSE 0 END) AS automation_impact_gbp_cumulative,
    SUM(CASE WHEN kpi_name = 'framework_adoption_proxy_pct' THEN value ELSE 0 END) AS framework_adoption_proxy_pct,
    SUM(CASE WHEN kpi_name = 'bi_utilisation_proxy_requests' THEN value ELSE 0 END) AS bi_utilisation_proxy_requests,
    SUM(CASE WHEN kpi_name = 'stakeholder_satisfaction_proxy_rating' THEN value ELSE 0 END) AS stakeholder_satisfaction_proxy_rating
FROM fact_kpi_daily
GROUP BY date;

CREATE VIEW IF NOT EXISTS vw_api_usage_daily AS
SELECT
    DATE(requested_at) AS date,
    endpoint,
    method,
    role,
    COUNT(*) AS request_count,
    AVG(status_code) AS avg_status_code
FROM api_usage_log
GROUP BY DATE(requested_at), endpoint, method, role;
