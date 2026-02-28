# dbt Pipeline - `analytics/`

This directory contains the full dbt project (`fintech_modeling`) that transforms raw fintech CSV data into a structured, tested, analytics-ready data model. It follows a layered architecture: **sources → staging → core marts → intermediate → KPIs**.

---

## Project Overview

| Property | Value |
|---|---|
| dbt project name | `fintech_modeling` |
| Database | DuckDB (`fintech.db`) |
| dbt adapter | `dbt-duckdb` |
| Packages | `dbt-utils` |
| Target schema | `dev` |

---

## Layer Architecture

```
sources (raw CSV)
    └── staging/
            └── stg_fintech_data          ← single cleaned staging model
                    └── marts/core/
                            ├── dim_customer
                            ├── dim_account
                            └── fct_customer_account_snapshot
                                    └── marts/intermediate/
                                                └── int_customer_account_metrics
                                                        └── marts/kpis/
                                                                ├── kpi_balance_deciles
                                                                ├── kpi_leverage_by_risk_tolerance
                                                                ├── kpi_portfolio_by_risk_tolerance
                                                                └── kpi_wealth_concentration
```

---

## Source Data

**File:** `data/raw/fintech_data.csv`  
**Registered in:** `models/sources/raw_sources.yml`

The raw CSV uses a namespaced column structure across four logical groups:

| Namespace | Key Columns |
|---|---|
| `Customer Profile` | `customer_id`, `age`, `occupation`, `risk_tolerance`, `investment_goals`, `education_level`, `marital_status`, `dependents`, `region`, `financial_history`, `sector`, `income_level`, `employment_history` |
| `Account Activity` | `account_id`, `balance`, `deposits`, `withdrawals`, `transfers`, `international_transfers`, `investments`, `account_type`, `transaction_threshold` |
| `Loan Application Summary` | `loan_amount`, `loan_purpose`, `employment_status`, `loan_term`, `interest_rate`, `loan_status` |
| `Time Series Data` | `dates`, `transactions`, `repayments` (Python-serialised strings) |

Note: `Account Activity.customer_id` is dropped in staging - it was verified to have zero mismatches against `Customer Profile.customer_id`.

---

## Models

### Staging - `models/staging/`

#### `stg_fintech_data`

The single entry point for all raw data. This model:
- Renames all namespaced columns to clean `snake_case`
- Drops the redundant `Account Activity.customer_id` field
- Preserves the original grain: **1 row per `customer_id` + `account_id`**
- Applies no aggregations or business logic - purely structural cleanup

**Key tests:**
- `customer_id` and `account_id`: `not_null`, `unique`
- Composite grain: `dbt_utils.unique_combination_of_columns(customer_id, account_id)`
- `account_type`: accepted values - `checking`, `savings`, `credit`, `investment`
- `risk_tolerance`: accepted values - `Low`, `Medium`, `High`
- `loan_status`: accepted values - `approved`, `pending`, `rejected`
- `balance`, `deposits`, `withdrawals`, `transfers`, `investments`: `dbt_utils.accepted_range(min: 0)`
- `interest_rate`: `dbt_utils.accepted_range(min: 0, max: 1)` - stored as decimal fraction
- `loan_amount`: `dbt_utils.accepted_range(min: 0.0000001)` - must be positive

---

### Core Marts - `models/marts/core/`

The core layer splits the staging model into canonical dimensions and a fact table following a simple star schema.

#### `dim_customer`

**Grain:** 1 row per `customer_id`  
**Purpose:** Customer descriptive attributes - no measures.

| Column | Description |
|---|---|
| `customer_id` | Primary key |
| `age` | Customer age |
| `occupation` | Job/profession |
| `risk_tolerance` | Low / Medium / High |
| `investment_goals` | Customer-stated investment objective |
| `education_level` | Highest education attained |
| `marital_status` | Marital status |
| `dependents` | Number of dependants |
| `region` | Geographic region |
| `sector` | Industry sector |
| `income_level` | Annual income |
| `employment_history` | Employment tenure |
| `employment_status` | Current employment status |
| `financial_history` | Credit/financial history flag |

**Tests:** `not_null` + `unique` on `customer_id`

---

#### `dim_account`

**Grain:** 1 row per `account_id`  
**Purpose:** Account-level product descriptors - no measures.

| Column | Description |
|---|---|
| `account_id` | Primary key |
| `account_type` | Product type (checking / savings / credit / investment) |
| `transaction_threshold` | Account transaction limit |

**Tests:** `not_null` + `unique` on `account_id`

---

#### `fct_customer_account_snapshot`

**Grain:** 1 row per `customer_id` + `account_id` combination  
**Purpose:** Raw financial measures associated with each account snapshot. Contains no business logic - all derivations happen downstream.

| Column | Type | Description |
|---|---|---|
| `customer_id` | FK | Foreign key to `dim_customer` |
| `account_id` | FK | Foreign key to `dim_account` |
| `balance` | Measure | Account balance |
| `deposits` | Measure | Aggregate deposits |
| `withdrawals` | Measure | Aggregate withdrawals |
| `transfers` | Measure | Aggregate transfers |
| `international_transfers` | Measure | International transfer total |
| `investments` | Measure | Aggregate investment amount |
| `loan_amount` | Measure | Outstanding loan principal |
| `interest_rate` | Measure | Rate as decimal (e.g. 0.08 = 8%) |
| `loan_purpose` | Descriptor | Purpose of loan |
| `loan_term` | Descriptor | Loan duration |
| `loan_status` | Descriptor | approved / pending / rejected |
| `transaction_description` | Descriptor | Transaction label |
| `ts_dates`, `ts_transactions`, `ts_repayments` | Raw blobs | Python-serialised time series strings (unparsed) |

**Tests:**
- `dbt_utils.unique_combination_of_columns(customer_id, account_id)`
- `customer_id` relationship to `dim_customer`
- `account_id` relationship to `dim_account`

---

### Intermediate - `models/marts/intermediate/`

#### `int_customer_account_metrics`

**Grain:** 1 row per `customer_id` + `account_id`  
**Purpose:** Joins `fct_customer_account_snapshot` with `dim_customer` and `dim_account`, and computes derived per-account metrics. This is the primary table queried by the Streamlit dashboard.

**Joins:**
- `fct` → `dim_customer` on `customer_id`
- `fct` → `dim_account` on `account_id`

**Sliceable descriptors (from dims):**
- `risk_tolerance`, `region`, `account_type`, `income_level`

**Derived metrics:**

| Column | Formula | Description |
|---|---|---|
| `net_flow` | `deposits - withdrawals` | Net money movement per account |
| `loan_to_income` | `loan_amount / income_level` | Leverage relative to income |
| `loan_to_balance` | `loan_amount / balance` | Loan exposure relative to account balance |
| `investment_to_balance` | `investments / balance` | Investment intensity relative to balance |

**Tests:** `not_null` on `customer_id`, `account_id`, `loan_to_income`, `investment_to_balance`  
`dbt_utils.unique_combination_of_columns(customer_id, account_id)`

---

### KPI Models - `models/marts/kpis/`

Pre-aggregated analytical views consumed directly by the dashboard or for standalone reporting.

#### `kpi_wealth_concentration`

Computes portfolio-level wealth concentration metrics using the top 10% of accounts by balance.

| Column | Description |
|---|---|
| `total_balance` | Total balance across all accounts |
| `top_10_balance` | Combined balance of the wealthiest 10% of accounts |
| `top_10_share` | `top_10_balance / total_balance` - the concentration ratio |

---

#### `kpi_balance_deciles`

Distributes all accounts into 10 equal groups (deciles) ordered by balance descending. Decile 1 = the highest-balance accounts.

| Column | Description |
|---|---|
| `balance_decile` | Decile rank (1–10, 1 = richest) |
| `accounts` | Number of accounts in this decile |
| `total_balance` | Total balance in this decile |
| `avg_balance` | Average balance in this decile |

---

#### `kpi_portfolio_by_risk_tolerance`

Aggregates portfolio KPIs grouped by customer risk appetite tier.

| Column | Description |
|---|---|
| `risk_tolerance` | Low / Medium / High |
| `accounts` | Account count |
| `total_balance` | Combined balance |
| `avg_balance` | Average balance |
| `total_loan_amount` | Total outstanding loans |
| `avg_net_flow` | Average net contributions |
| `weighted_avg_interest_rate` | Loan-weighted average interest rate |

---

#### `kpi_leverage_by_risk_tolerance`

Groups accounts by risk tolerance and analyses leverage distribution using loan-to-income quartiles.

| Column | Description |
|---|---|
| `risk_tolerance` | Low / Medium / High |
| `accounts` | Account count |
| `avg_income` | Average customer income |
| `avg_loan_amount` | Average loan principal |
| `avg_loan_to_income` | Average leverage ratio |
| `pct_top_quartile_leverage` | Share of accounts in the highest loan-to-income quartile (Q4) |

---

## Testing Strategy

dbt tests are defined in YAML schema files alongside each model. The project uses both built-in dbt tests and `dbt-utils` extended tests.

| Test type | Used on |
|---|---|
| `not_null` | All primary keys and critical measures |
| `unique` | All primary keys |
| `accepted_values` | `account_type`, `risk_tolerance`, `loan_status` |
| `dbt_utils.accepted_range` | All financial measures (balance, deposits, rates) |
| `dbt_utils.unique_combination_of_columns` | Composite grain on staging, fact, and intermediate |
| `relationships` | FK integrity between fact and dimensions |

---

## Running the Pipeline

```bash
cd analytics

# Install packages
dbt deps

# Build all models (writes to fintech.db - a DuckDB database file)
dbt run

# Run all tests
dbt test

# Build and test in one command
dbt build

# Run a specific model and its dependencies
dbt run --select int_customer_account_metrics+

# Run only KPI models
dbt run --select marts.kpis

# View lineage graph (requires dbt docs)
dbt docs generate
dbt docs serve
```

---

## Package Dependencies

**`dbt-utils`** - provides extended test types and macro utilities used across the project.

Installed via `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

Run `dbt deps` to install.
