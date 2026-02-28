# Streamlit Dashboard - `app.py`

An interactive portfolio analytics dashboard built with Streamlit, DuckDB, and Plotly. It queries the dbt-built SQLite database in real time and gives analysts a filterable view of portfolio health, growth drivers, and wealth concentration risk.

---

## Overview

The dashboard reads from `fintech.db` (built by the dbt pipeline) and surfaces three analytical sections behind a shared sidebar filter. All charts and KPIs update dynamically when filters are applied.

**Key design choice:** Both dbt and the Streamlit app use DuckDB as their engine - dbt writes all transformed models into `fintech.db` (a native DuckDB file), and the app connects to the same file directly at runtime using a read-only DuckDB connection. This keeps the dbt layer clean and generic while giving the dashboard full ad-hoc filter flexibility with no data movement or server round-trips.

---

## Running the Dashboard

### Prerequisites

```bash
pip install streamlit duckdb pandas plotly numpy
```

The dbt pipeline must have been run first to populate `fintech.db` (a DuckDB database). See [`analytics/README.md`](./analytics/README.md).

### Launch

```bash
streamlit run app.py
```

Opens at `http://localhost:8501` by default.

---

## Sidebar Filters

Three optional multiselect filters narrow the data shown across all sections simultaneously:

| Filter | Field | Description |
|---|---|---|
| Customer risk appetite | `risk_tolerance` | Low / Medium / High - the investment risk tier the customer selected |
| Region | `region` | Geographic region of the customer |
| Product type | `account_type` | Checking / Savings / Credit / Investment |

Leaving all filters blank shows the full portfolio. A **Clear filters** button resets all selections. Active filters are summarised at the bottom of the sidebar.

Filters are applied via a dynamically built `WHERE` clause injected into all DuckDB queries. The clause is constructed safely using value escaping to prevent injection issues.

---

## Dashboard Sections

### Dynamic Narrative Banner

Below the title, a contextual summary sentence is generated at runtime describing the portfolio state - total accounts, balance, flow direction, and concentration level - based on the active filter selection. If filters are active, a note indicates the view is a subset.

---

### Section 1 - Portfolio Overview

Six headline KPI metrics displayed as Streamlit metric cards:

| Metric | Description |
|---|---|
| Total accounts | Count of accounts in the selected portfolio |
| Total balance | Combined AUM across all selected accounts |
| Total credit exposure | Sum of outstanding loan principals |
| Balance-weighted product rate | Interest rate averaged by account balance - a directional signal for overall portfolio yield/cost |
| Total net contributions | `SUM(deposits - withdrawals)` - is the portfolio growing from customer activity? |
| Avg net contributions | Per-account average - shows typical account behaviour alongside the total |

---

### Section 2 - Segments & Growth Drivers

Answers the question: *which segments are growing and which are shrinking?*

**Net contributions by product type** - a bar chart showing `SUM(net_flow)` grouped by `account_type`. Positive bars mean more money is flowing in than out for that product. Negative bars signal net outflows.

**Net contributions by region** - the same analysis cut by geography, identifying which regions are driving growth or experiencing asset outflows.

**Customer risk appetite breakdown** - two bar charts (total balance and account count) always showing the **full portfolio**, not just the filtered selection. When a filter is active, the selected risk tiers are highlighted in blue and the rest shown in grey - so you can see your selection in context of the whole book.

---

### Section 3 - Concentration & Dependency Risk

Answers the question: *how dependent is the portfolio on a small number of large accounts?*

**Top 10% balance share** - the proportion of total portfolio balance held by the wealthiest 10% of accounts. A high share (above 60–70%) signals dependency risk: if those accounts churn, the impact is disproportionate.

**Gini coefficient** - a single number from 0 to 1 summarising the balance distribution. Computed in-app from a Lorenz curve query. Banded and labelled:

| Range | Label |
|---|---|
| < 0.35 | Relatively even distribution |
| 0.35 – 0.55 | Moderate concentration |
| 0.55 – 0.70 | High concentration |
| > 0.70 | Extreme concentration |

**Balance by account decile** - a bar chart splitting accounts into 10 equal groups by balance size (D1 = wealthiest 10%). Dark blue for D1, lighter blue for D2–D3, grey for the rest. A large gap between D1 and the rest is the key visual signal for concentration risk. An expandable table below shows the full min/avg/max/total breakdown per decile.

**Lorenz curve** - plots cumulative balance share against cumulative account share (poorest to richest). The closer the curve bows to the bottom-right corner (away from the diagonal line of perfect equality), the more concentrated the portfolio. The shaded area between the curve and the diagonal is proportional to the Gini coefficient.

---

## Architecture & Key Implementation Details

### Database connection

```python
@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)
```

A single DuckDB connection is opened once and cached for the app session. Since dbt also uses DuckDB as its database engine, `fintech.db` is a native DuckDB file - the app connects to it directly with no format conversion or data copying required.

### Query layer

All SQL runs against `dev.int_customer_account_metrics` as the primary base table. This intermediate model is the join of the fact table and both dimensions, enriched with derived metrics - making it the ideal single query surface for the dashboard.

KPI queries are run upfront at page load (not on demand), so all charts update together when filters change. DuckDB's columnar engine handles these aggregations efficiently even on larger datasets.

### Gini coefficient calculation

The Gini coefficient is computed in-app using a Lorenz curve SQL query and NumPy trapezoid integration:

```
Gini = 1 - 2 × (area under Lorenz curve)
```

The Lorenz curve itself is built in SQL using window functions (`ROW_NUMBER`, cumulative `SUM`) and then plotted directly with Plotly.

### Filter highlighting

The risk appetite charts always display the full portfolio. When risk tolerance filters are active, the `bar_chart()` helper colours the selected bars blue and unselected bars grey - letting the user see how their selection sits within the overall book rather than losing context.

### Formatting helpers

All monetary values are formatted consistently as `$X,XXX` via `fmt_money()`. Interest rates stored as decimals (e.g. `0.08`) are displayed as percentages (`8.00%`) via `fmt_pct_from_rate()`. A `fmt_pct()` helper handles values already in percentage form.

---

## File Structure

```
app.py              # Full dashboard - single-file Streamlit app
fintech.db          # SQLite database populated by dbt
```

---

## Extending the Dashboard

**Adding a new chart:** Query from `dev.int_customer_account_metrics` (or any dbt KPI model) using `con.execute(...)`. Use the `bar_chart()` helper for consistent bar chart styling, or build a custom Plotly figure.

**Adding a new filter:** Add a `multiselect` to the sidebar and extend `build_where_clause()` with the new field. All existing queries will automatically pick up the updated `where` variable.

**Adding a new KPI model:** Build the model in dbt under `models/marts/kpis/`, run `dbt run`, then query it directly in `app.py` using `con.execute("SELECT ... FROM dev.<model_name>")`.
