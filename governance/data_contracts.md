# Data Contracts

These contracts are validated by `etl/validate_contracts.py` before mart load.

## Principles

- Required columns must exist for each dimensional/fact table.
- Required fields must be non-null unless explicitly nullable.
- Numeric fields must be coercible to the expected type.
- Contract validation outcomes are persisted to `contract_validation_log`.

## Contract Scope

- Dimensions: `dim_date`, `dim_site`, `dim_customer`, `dim_team`, `dim_category`, `dim_carrier`, `dim_product`
- Facts: `fact_jobs`, `fact_incidents`, `fact_comms`, `fact_costs`, `fact_automation_events`
- Scenario metadata: `scenario_registry`

## Key Nullable Exceptions

- `fact_jobs.delivered_date_key` may be null for in-flight jobs and Scenario C quality injection.
- `fact_automation_events.notes` may be null.

## Type Rules

- Integer keys and counters: `int`
- Monetary and effort fields: `float`
- Categorical and identifiers: `str`

See `etl/validate_contracts.py` for executable contract definitions.
