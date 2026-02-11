# Data Quality Scorecard

- Run timestamp (UTC): 2026-02-11T13:30:19.733847+00:00
- Overall score: **99.81 / 100**
- Freshness lag (days): -8

## Check Results
- completeness (fact_jobs): PASS | score=99.66 | Delivered jobs with missing delivered_date_key penalized.
- freshness_lag (dim_date): PASS | score=100.0 | Latest data date 2026-02-19 (lag -8 day(s))
- duplicates (fact_jobs): PASS | score=99.57 | Duplicate penalty derived from duplicate_flag prevalence.
- null_key_fields (fact_jobs|fact_comms|fact_incidents): PASS | score=100.0 | Nulls in critical key fields across operational facts.
- schema_drift (warehouse): PASS | score=100.0 | No schema drift detected
- out_of_range_values (fact_jobs|fact_comms|fact_incidents): PASS | score=100.0 | Negative / impossible values in key numeric columns.

## Notable Daily Issues
- 2026-01-30: missing_delivered=22, duplicate_score=66.67, overall=84.33
- 2026-01-31: missing_delivered=17, duplicate_score=82.14, overall=88.45