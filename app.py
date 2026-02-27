import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Fintech Portfolio Dashboard", layout="wide")
st.title("Fintech Portfolio Dashboard")

DB_PATH = "fintech.db"
BASE = "dev.int_customer_account_metrics"
CURRENCY = "$"  # US Dollars


@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


con = get_con()
st.caption(f"Connected to: {DB_PATH}")


def fmt_money(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{CURRENCY}{x:,.0f}"


def fmt_pct_from_rate(x, decimals=2):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{x * 100:.{decimals}f}%"


def fmt_pct(x, decimals=2):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "—"
    return f"{x:.{decimals}%}"


def sql_in(values):
    escaped = [str(v).replace("'", "''") for v in values]
    return "(" + ", ".join([f"'{v}'" for v in escaped]) + ")"


def build_where_clause(risks_selected, regions_selected, accts_selected):
    clauses = []

    if risks_selected:
        clauses.append(f"risk_tolerance IN {sql_in(risks_selected)}")

    if regions_selected:
        clauses.append(f"region IN {sql_in(regions_selected)}")

    if accts_selected:
        clauses.append(f"account_type IN {sql_in(accts_selected)}")

    if not clauses:
        return ""

    return "WHERE " + " AND ".join(clauses)


@st.cache_data
def load_filter_options():
    q = f"""
        SELECT DISTINCT
            risk_tolerance,
            region,
            account_type
        FROM {BASE}
    """
    df = con.execute(q).fetchdf()
    risks = sorted([x for x in df["risk_tolerance"].dropna().unique().tolist()])
    regions = sorted([x for x in df["region"].dropna().unique().tolist()])
    acct_types = sorted([x for x in df["account_type"].dropna().unique().tolist()])
    return risks, regions, acct_types


risks, regions, acct_types = load_filter_options()

st.sidebar.header("Filters")

if st.sidebar.button("Clear filters", use_container_width=True):
    st.session_state["risk_tolerance_sel"] = []
    st.session_state["region_sel"] = []
    st.session_state["account_type_sel"] = []
    st.rerun()

sel_risks = st.sidebar.multiselect(
    "Risk tolerance",
    options=risks,
    default=risks,
    key="risk_tolerance_sel",
)

sel_regions = st.sidebar.multiselect(
    "Region",
    options=regions,
    default=regions,
    key="region_sel",
)

sel_accts = st.sidebar.multiselect(
    "Account type",
    options=acct_types,
    default=acct_types,
    key="account_type_sel",
)

where = build_where_clause(sel_risks, sel_regions, sel_accts)

# Portfolio overview
overview_q = f"""
SELECT
    COUNT(*) AS accounts,
    SUM(balance) AS total_balance,
    SUM(loan_amount) AS total_loan_amount,
    CASE
        WHEN SUM(balance) = 0 THEN NULL
        ELSE SUM(interest_rate * balance) / SUM(balance)
    END AS weighted_avg_interest_rate,
    AVG(net_flow) AS avg_net_flow
FROM {BASE}
{where}
"""

overview_df = con.execute(overview_q).fetchdf()
overview = overview_df.iloc[0].to_dict() if not overview_df.empty else {}

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Accounts", f"{int(overview.get('accounts', 0) or 0):,}")
c2.metric("Total balance", fmt_money(overview.get("total_balance")))
c3.metric("Total loan amount", fmt_money(overview.get("total_loan_amount")))
c4.metric("Weighted avg interest", fmt_pct_from_rate(overview.get("weighted_avg_interest_rate"), 2))
c5.metric("Avg net flow", fmt_money(overview.get("avg_net_flow")))

st.divider()

# Risk segmentation
st.subheader("Risk Segmentation (Live)")

risk_q = f"""
SELECT
    risk_tolerance,
    COUNT(*) AS accounts,
    SUM(balance) AS total_balance,
    AVG(balance) AS avg_balance,
    SUM(loan_amount) AS total_loan_amount,
    AVG(net_flow) AS avg_net_flow,
    CASE
        WHEN SUM(balance) = 0 THEN NULL
        ELSE SUM(interest_rate * balance) / SUM(balance)
    END AS weighted_avg_interest_rate
FROM {BASE}
{where}
GROUP BY 1
ORDER BY 1
"""

risk_df = con.execute(risk_q).fetchdf()

risk_display = risk_df.copy()
for col in ["total_balance", "avg_balance", "total_loan_amount", "avg_net_flow"]:
    if col in risk_display.columns:
        risk_display[col] = risk_display[col].map(fmt_money)
if "weighted_avg_interest_rate" in risk_display.columns:
    risk_display["weighted_avg_interest_rate"] = risk_display["weighted_avg_interest_rate"].map(
        lambda x: fmt_pct_from_rate(x, 2)
    )

risk_display = risk_display.rename(columns={
    "risk_tolerance": "risk",
    "total_balance": "total balance",
    "avg_balance": "avg balance",
    "total_loan_amount": "total loan",
    "avg_net_flow": "avg net flow",
    "weighted_avg_interest_rate": "wtd avg interest",
})

st.dataframe(risk_display, use_container_width=True, hide_index=True)

st.divider()

# Wealth concentration
st.subheader("Wealth Concentration (Live)")

wealth_q = f"""
WITH base AS (
    SELECT balance
    FROM {BASE}
    {where}
),
ranked AS (
    SELECT
        balance,
        ROW_NUMBER() OVER (ORDER BY balance DESC) AS rn,
        COUNT(*) OVER () AS n,
        SUM(balance) OVER () AS total_balance
    FROM base
),
top10 AS (
    SELECT
        total_balance,
        SUM(CASE WHEN rn <= CEIL(n * 0.10) THEN balance ELSE 0 END) AS top_10_balance
    FROM ranked
    GROUP BY total_balance
)
SELECT
    total_balance,
    top_10_balance,
    CASE WHEN total_balance = 0 THEN NULL ELSE top_10_balance / total_balance END AS top_10_share
FROM top10
"""

wealth_df = con.execute(wealth_q).fetchdf()
wealth = wealth_df.iloc[0].to_dict() if not wealth_df.empty else {}

w1, w2, w3 = st.columns(3)
w1.metric("Total balance", fmt_money(wealth.get("total_balance")))
w2.metric("Top 10% balance", fmt_money(wealth.get("top_10_balance")))
w3.metric("Top 10% share", fmt_pct(wealth.get("top_10_share"), 2))

# Balance deciles
deciles_q = f"""
WITH base AS (
    SELECT balance
    FROM {BASE}
    {where}
),
deciled AS (
    SELECT
        balance,
        NTILE(10) OVER (ORDER BY balance DESC) AS decile
    FROM base
)
SELECT
    decile,
    COUNT(*) AS accounts,
    MIN(balance) AS min_balance,
    AVG(balance) AS avg_balance,
    MAX(balance) AS max_balance,
    SUM(balance) AS total_balance
FROM deciled
GROUP BY decile
ORDER BY decile
"""

deciles_df = con.execute(deciles_q).fetchdf()

deciles_display = deciles_df.copy()
if "decile" in deciles_display.columns:
    deciles_display["decile"] = deciles_display["decile"].astype(int).map(lambda d: f"D{d}")

for col in ["min_balance", "avg_balance", "max_balance", "total_balance"]:
    if col in deciles_display.columns:
        deciles_display[col] = deciles_display[col].map(fmt_money)

deciles_display = deciles_display.rename(columns={
    "decile": "decile (D1 richest)",
    "min_balance": "min",
    "avg_balance": "avg",
    "max_balance": "max",
    "total_balance": "total",
})

st.dataframe(deciles_display, use_container_width=True, hide_index=True)

if not deciles_df.empty:
    chart_df = deciles_df.copy()
    chart_df["decile"] = chart_df["decile"].astype(int)
    st.bar_chart(chart_df.set_index("decile")["total_balance"])

st.divider()

# Lorenz curve + Gini
st.subheader("Wealth Inequality (Lorenz Curve + Gini)")

and_or_where = "AND" if where else "WHERE"

lorenz_q = f"""
WITH base AS (
    SELECT balance
    FROM {BASE}
    {where}
    {and_or_where} balance IS NOT NULL
    AND balance >= 0
),
ordered AS (
    SELECT
        balance,
        ROW_NUMBER() OVER (ORDER BY balance ASC) AS rn,
        COUNT(*) OVER () AS n,
        SUM(balance) OVER () AS total
    FROM base
),
cum AS (
    SELECT
        rn,
        n,
        total,
        SUM(balance) OVER (ORDER BY rn) AS cum_balance
    FROM ordered
)
SELECT
    rn,
    n,
    CASE WHEN n = 0 THEN NULL ELSE rn::DOUBLE / n END AS cum_pop_share,
    CASE WHEN total = 0 THEN NULL ELSE cum_balance::DOUBLE / total END AS cum_wealth_share
FROM cum
ORDER BY rn
"""

lorenz_df = con.execute(lorenz_q).fetchdf()

if lorenz_df.empty or lorenz_df["cum_pop_share"].isna().all():
    st.info("Not enough data after filters to compute Lorenz curve / Gini.")
else:
    start = pd.DataFrame({
        "cum_pop_share": [0.0],
        "cum_wealth_share": [0.0],
    })

    curve = pd.concat(
        [start, lorenz_df[["cum_pop_share", "cum_wealth_share"]]],
        ignore_index=True,
    )

    area = float(
        (curve["cum_wealth_share"] * curve["cum_pop_share"].diff().fillna(0)).sum()
    )
    gini = 1 - 2 * area

    g1, g2 = st.columns([1, 3])
    g1.metric("Gini coefficient", f"{gini:.3f}")

    plot_df = curve.rename(columns={
        "cum_pop_share": "Cumulative population share",
        "cum_wealth_share": "Cumulative wealth share",
    })

    g2.line_chart(plot_df.set_index("Cumulative population share"))

    st.caption(
        "Lorenz curve shows cumulative wealth vs cumulative population (sorted by balance). "
        "Gini ranges from 0 (perfect equality) to 1 (maximum inequality)."
    )
