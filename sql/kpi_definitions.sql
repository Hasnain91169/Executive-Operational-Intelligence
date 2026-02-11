INSERT OR REPLACE INTO kpi_definitions (
    kpi_name, description, formula_text, grain, owner_role, threshold_good, threshold_bad, refresh_cadence, business_value, leading_indicator_bool
) VALUES
('on_time_delivery_pct', 'Percentage of jobs delivered on or before promised date.', '100 * delivered_on_time_jobs / delivered_jobs', 'daily', 'ops', 95, 88, 'daily', 'Protects customer trust and delivery reliability.', 0),
('sla_breach_rate_pct', 'SLA-sensitive communication breach rate.', '100 * breached_sla_sensitive_comms / sla_sensitive_comms', 'daily', 'customer_service', 5, 12, 'daily', 'Reduces escalations and improves customer response SLAs.', 1),
('exception_rate_per_100_jobs', 'Operational incidents per 100 jobs.', '100 * incident_count / total_jobs', 'daily', 'ops', 6, 12, 'daily', 'Highlights disruption pressure and execution risk.', 1),
('manual_workload_hours_weekly', 'Manual workload hours derived from communication effort.', 'SUM(comms_minutes)/60 over trailing 7 days', 'daily', 'ops', 120, 180, 'daily', 'Tracks workload and capacity strain for teams.', 1),
('cost_leakage_estimate_gbp', 'Estimated avoidable cost from comms and incident effort.', '((comms_minutes + incident_minutes_lost)/60) * blended_rate', 'daily', 'finance', 1800, 3500, 'daily', 'Quantifies operational inefficiency in GBP terms.', 0),
('data_quality_score', 'Composite data quality score (0-100).', '100 - weighted_penalties(nulls, freshness, duplicates, schema, ranges)', 'daily', 'bi_lead', 92, 80, 'daily', 'Builds trust in metrics and governance compliance.', 1),
('automation_impact_hours_weekly', 'Weekly hours saved through automations.', 'SUM(hours_saved) over trailing 7 days', 'daily', 'ops', 18, 8, 'weekly', 'Demonstrates automation productivity gains.', 0),
('automation_impact_gbp_weekly', 'Weekly GBP savings from automations.', 'SUM(gbp_saved) over trailing 7 days', 'daily', 'finance', 2200, 900, 'weekly', 'Shows direct financial return from automation.', 0),
('automation_impact_gbp_cumulative', 'Cumulative automation GBP savings.', 'RUNNING_SUM(gbp_saved)', 'daily', 'finance', 18000, 6000, 'daily', 'Tracks long-run value capture from automation program.', 0),
('framework_adoption_proxy_pct', 'Proxy for governance framework adoption completeness.', '100 * kpis_with_owner_threshold_refresh / total_kpis', 'daily', 'bi_lead', 95, 80, 'daily', 'Measures KPI governance maturity across the portfolio.', 1),
('bi_utilisation_proxy_requests', 'Proxy for BI utilisation from API traffic.', 'COUNT(api_requests) by day', 'daily', 'bi_lead', 40, 10, 'daily', 'Signals stakeholder engagement with analytics assets.', 1),
('stakeholder_satisfaction_proxy_rating', 'Average stakeholder rating on explain insights (1-5).', 'AVG(feedback.rating)', 'daily', 'bi_lead', 4.2, 3.4, 'daily', 'Reflects perceived utility and trust in insight quality.', 0);
