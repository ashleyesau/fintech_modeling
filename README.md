# Fintech Portfolio Analytics

A end-to-end data analytics project simulating a fintech company's data stack - from raw CSV ingestion through a layered dbt transformation pipeline to an interactive Streamlit dashboard with real-time filtering, wealth concentration analysis, and portfolio KPIs.

---

## What This Project Does

This project takes a single flat CSV of simulated fintech customer and account data and transforms it into a structured analytics pipeline with a production-style dashboard. It demonstrates how a modern analytics engineering workflow connects raw data to business insight.

The pipeline answers questions like:
- How concentrated is the portfolio? Do a small number of accounts hold most of the value?
- Which customer segments and product types are growing vs. shrinking?
- How does leverage (loan-to-income) differ across risk appetite tiers?
- What does the full balance distribution look like across account deciles?

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data transformation | [dbt Core](https://www.getdbt.com/) with DuckDB adapter |
| Analytics database | [DuckDB](https://duckdb.org/) (`fintech.db`) |
| In-app query engine | DuckDB (same connection, read-only) |
| Dashboard | [Streamlit](https://streamlit.io/) |
| Visualisations | [Plotly](https://plotly.com/python/) |
| dbt testing utilities | [dbt-utils](https://github.com/dbt-labs/dbt-utils) |
| Language | Python 3 |

---

## Architecture

```
data/raw/fintech_data.csv
        │
        ▼
┌──────────────────────────────────────┐
│           dbt Pipeline               │
│                                      │
│  sources (raw CSV)                   │
│       │                              │
│       ▼                              │
│  staging/stg_fintech_data            │  ← rename, clean, validate
│       │                              │
│       ▼                              │
│  marts/core/                         │
│    dim_customer                      │  ← customer attributes
│    dim_account                       │  ← account descriptors
│    fct_customer_account_snapshot     │  ← grain: customer + account
│       │                              │
│       ▼                              │
│  marts/intermediate/                 │
│    int_customer_account_metrics      │  ← joins dims + fact, adds derived metrics
│       │                              │
│       ▼                              │
│  marts/kpis/                         │
│    kpi_balance_deciles               │
│    kpi_leverage_by_risk_tolerance    │
│    kpi_portfolio_by_risk_tolerance   │
│    kpi_wealth_concentration          │
└──────────────────────────────────────┘
        │
        ▼
   fintech.db  (SQLite)
        │
        ▼
┌──────────────────────┐
│   app.py             │  ← Streamlit + DuckDB
│   Streamlit Dashboard│
└──────────────────────┘
```

---

## Project Structure

```
fintech_modeling/
├── app.py                        # Streamlit dashboard
├── fintech.db                    # SQLite database (dbt target)
├── data/
│   └── raw/
│       └── fintech_data.csv      # Source data
├── analytics/                    # dbt project
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources/              # Source definitions
│   │   ├── staging/              # Cleaned, renamed staging layer
│   │   └── marts/
│   │       ├── core/             # Dimensions + fact table
│   │       ├── intermediate/     # Joined, enriched metrics layer
│   │       └── kpis/             # Aggregated KPI models
│   └── packages.yml
└── logs/
```

---

## Quickstart

### Prerequisites

- Python 3.9+
- dbt Core with SQLite adapter
- pip packages: `streamlit`, `duckdb`, `pandas`, `plotly`

### 1. Install dependencies

```bash
pip install streamlit duckdb pandas plotly
pip install dbt-core dbt-duckdb
```

### 2. Run the dbt pipeline

```bash
cd analytics
dbt deps          # install dbt-utils package
dbt run           # build all models
dbt test          # run data quality tests
```

This populates `fintech.db` (a DuckDB database file) with all staging, mart, intermediate, and KPI tables.

### 3. Launch the dashboard

```bash
cd ..
streamlit run app.py
```

The dashboard will open at `http://localhost:8501`.

---

## Dashboard Sections

The Streamlit app has three main sections, all driven by a sidebar filter (risk appetite, region, product type):

**1. Portfolio Overview** - headline KPIs including total accounts, AUM, credit exposure, balance-weighted interest rate, and net contributions.

**2. Segments & Growth Drivers** - net contribution flows broken down by product type and region, plus a full-portfolio risk appetite breakdown with active filter highlighting.

**3. Concentration & Dependency Risk** - top-10% balance share, Gini coefficient, balance decile chart, and an interactive Lorenz curve showing the full distribution of wealth across accounts.

---

## Documentation

- [dbt Pipeline - analytics/README.md](./analytics/README.md)
- [Streamlit Dashboard - DASHBOARD.md](./DASHBOARD.md)

---

## Data Source

The source data (`fintech_data.csv`) is a synthetic flat-file dataset simulating a fintech company's customer, account, loan, and transaction records. It contains nested namespaced columns (e.g. `Customer Profile.customer_id`, `Account Activity.balance`) which are flattened and renamed in the staging layer. dbt reads this file via a DuckDB source and writes all transformed models back into `fintech.db`.
