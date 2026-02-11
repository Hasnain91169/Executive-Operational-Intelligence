# Power BI Desktop Guide (Local-Only)

This MVP is designed for **Power BI Desktop without a tenant**.

## 1) Prepare Data

```bash
python etl/generate_sample_data.py
python etl/transform.py
python etl/load_mart.py
```

This creates CSV exports in `data/mart/` for direct import.

## 2) Connect in Power BI Desktop

## Option A (Recommended): Import CSVs

1. `Get Data` -> `Text/CSV`
2. Import these tables from `data/mart/`:
   - `dim_date.csv`
   - `dim_site.csv`
   - `dim_customer.csv`
   - `dim_team.csv`
   - `dim_category.csv`
   - `dim_carrier.csv`
   - `dim_product.csv`
   - `fact_jobs.csv`
   - `fact_incidents.csv`
   - `fact_comms.csv`
   - `fact_costs.csv`
   - `fact_automation_events.csv`
   - `fact_kpi_daily.csv`
   - `kpi_definitions.csv`

## Option B: SQLite via ODBC (if installed)

1. Install SQLite ODBC driver.
2. Configure DSN for `data/mart/ops_copilot.db`.
3. In Power BI: `Get Data` -> `ODBC` -> select DSN.

## 3) Recreate Star Schema Relationships

Use **single direction** from dimensions to facts.

- `dim_date[date_key]` -> `fact_jobs[date_key]`
- `dim_date[date_key]` -> `fact_incidents[date_key]`
- `dim_date[date_key]` -> `fact_comms[date_key]`
- `dim_date[date_key]` -> `fact_costs[date_key]`
- `dim_date[date_key]` -> `fact_automation_events[date_key]`
- `dim_date[date_key]` -> `fact_kpi_daily[date_key]`
- `dim_site[site_id]` -> each fact with `site_id`
- `dim_customer[customer_id]` -> `fact_jobs`, `fact_comms`
- `dim_team[team_id]` -> `fact_jobs`
- `dim_category[category_id]` -> `fact_comms`
- `dim_carrier[carrier_id]` -> `fact_jobs`, `fact_comms`
- `dim_product[product_id]` -> `fact_jobs`, `fact_incidents`, `fact_comms`

## 4) Suggested DAX Measures (Enterprise Demo Set)

```DAX
On Time Delivery % =
DIVIDE(
    CALCULATE(COUNTROWS(fact_jobs), NOT(ISBLANK(fact_jobs[delivered_date_key])), fact_jobs[delivered_date_key] <= fact_jobs[promised_date_key]),
    CALCULATE(COUNTROWS(fact_jobs), NOT(ISBLANK(fact_jobs[delivered_date_key])))
) * 100
```

```DAX
SLA Breach Rate % =
DIVIDE(
    CALCULATE(COUNTROWS(fact_comms), fact_comms[sla_sensitive_bool] = 1, fact_comms[breached_bool] = 1),
    CALCULATE(COUNTROWS(fact_comms), fact_comms[sla_sensitive_bool] = 1)
) * 100
```

```DAX
Exception Rate per 100 Jobs =
DIVIDE(COUNTROWS(fact_incidents), COUNTROWS(fact_jobs)) * 100
```

```DAX
Manual Workload Hours = DIVIDE(SUM(fact_comms[minutes_spent]), 60)
```

```DAX
Manual Workload 7D Avg =
AVERAGEX(
    DATESINPERIOD(dim_date[date], MAX(dim_date[date]), -7, DAY),
    [Manual Workload Hours]
)
```

```DAX
Cost Leakage Estimate GBP =
DIVIDE(SUM(fact_comms[minutes_spent]) + SUM(fact_incidents[minutes_lost]), 60) * 42
```

```DAX
Automation Impact GBP Weekly =
CALCULATE(
    SUM(fact_automation_events[gbp_saved]),
    DATESINPERIOD(dim_date[date], MAX(dim_date[date]), -7, DAY)
)
```

```DAX
Automation Impact GBP Cumulative =
CALCULATE(
    SUM(fact_automation_events[gbp_saved]),
    FILTER(ALL(dim_date[date]), dim_date[date] <= MAX(dim_date[date]))
)
```

```DAX
KPI Value = SUM(fact_kpi_daily[value])
```

```DAX
WoW Delta % =
VAR CurrentWeek = [KPI Value]
VAR PrevWeek = CALCULATE([KPI Value], DATEADD(dim_date[date], -7, DAY))
RETURN DIVIDE(CurrentWeek - PrevWeek, PrevWeek) * 100
```

```DAX
Threshold Flag =
VAR v = [KPI Value]
VAR Good = MAX(fact_kpi_daily[target_good])
VAR Bad = MAX(fact_kpi_daily[target_bad])
RETURN IF(v >= Good, "Good", IF(v >= Bad, "Watch", "Bad"))
```

## 5) Suggested Report Pages

- Exec Overview: KPI cards + trend lines + anomaly table
- SLA Drillthrough: site/carrier/category breakdown
- Exceptions Drillthrough: product/incident-type matrix
- Governance: data quality score + ownership completeness + usage proxy
- Automation Impact: runs, savings, and cumulative value

## 6) Screenshot Checklist (Placeholders)

- `[Screenshot: Exec Overview KPI board]`
- `[Screenshot: SLA anomaly and top drivers]`
- `[Screenshot: Exceptions by product/site]`
- `[Screenshot: Governance scorecard page]`
- `[Screenshot: Automation impact page]`
