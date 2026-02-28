import streamlit as st
import duckdb
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(page_title="Fintech Portfolio Dashboard", layout="wide")

DB_PATH = "fintech.db"
BASE = "dev.int_customer_account_metrics"
CURRENCY = "$"


@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


con = get_con()


# --- Formatting helpers ---
def fmt_money(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "-"
    return f"{CURRENCY}{x:,.0f}"


def fmt_pct_from_rate(x, decimals=2):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "-"
    return f"{x * 100:.{decimals}f}%"


def fmt_pct(x, decimals=2):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "-"
    return f"{x:.{decimals}%}"


def gini_label(g):
    if g is None:
        return "-"
    if g < 0.35:
        return "Relatively even distribution"
    elif g < 0.55:
        return "Moderate concentration"
    elif g < 0.70:
        return "High concentration"
    else:
        return "Extreme concentration"


def sql_in(values):
    escaped = [str(v).replace("'", "''") for v in values]
    return "(" + ", ".join([f"'{v}'" for v in escaped]) + ")"


def build_where_clause(risks_selected, regions_selected, accts_selected):
    clauses = []
    if risks_selected and len(risks_selected) < len(risks):
        clauses.append(f"risk_tolerance IN {sql_in(risks_selected)}")
    if regions_selected and len(regions_selected) < len(regions):
        clauses.append(f"region IN {sql_in(regions_selected)}")
    if accts_selected and len(accts_selected) < len(acct_types):
        clauses.append(f"account_type IN {sql_in(accts_selected)}")
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


def bar_chart(df, x_col, y_col, title, active_set=None, fmt_fn=None):
    """Render a Plotly bar chart. If active_set provided, highlight matching x values in blue."""
    if active_set is not None:
        colours = ["#2563eb" if str(r) in active_set else "#d1d5db" for r in df[x_col]]
    else:
        colours = "#2563eb"
    text = [fmt_fn(v) if fmt_fn else str(v) for v in df[y_col]]
    fig = go.Figure(go.Bar(
        x=df[x_col], y=df[y_col],
        marker_color=colours,
        text=text, textposition="outside",
        width=0.4,
    ))
    fig.update_layout(
        title=title, showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        margin=dict(t=40, b=20, l=20, r=20),
    )
    return fig


# --- Filter options ---
@st.cache_data
def load_filter_options():
    df = con.execute(f"SELECT DISTINCT risk_tolerance, region, account_type FROM {BASE}").fetchdf()
    risks = sorted(df["risk_tolerance"].dropna().unique().tolist())
    regions = sorted(df["region"].dropna().unique().tolist())
    acct_types = sorted(df["account_type"].dropna().unique().tolist())
    return risks, regions, acct_types


risks, regions, acct_types = load_filter_options()

# --- Sidebar ---
st.sidebar.header("Filters")
st.sidebar.caption("Narrow the data shown across all sections. Leave blank to see the full portfolio.")

if st.session_state.get("_reset_filters"):
    st.session_state["_reset_filters"] = False
    st.session_state["risk_tolerance_sel"] = []
    st.session_state["region_sel"] = []
    st.session_state["account_type_sel"] = []

sel_risks = st.sidebar.multiselect(
    "Customer risk appetite",
    options=risks,
    key="risk_tolerance_sel",
    help="Filter by the risk profile the customer selected when opening their account.",
)
sel_regions = st.sidebar.multiselect(
    "Region",
    options=regions,
    key="region_sel",
    help="Filter by the customer's geographic region.",
)
sel_accts = st.sidebar.multiselect(
    "Product type",
    options=acct_types,
    key="account_type_sel",
    help="Filter by the type of account (e.g. Savings, Investment, Loan).",
)

if st.sidebar.button("Clear filters", use_container_width=True):
    st.session_state["_reset_filters"] = True
    st.rerun()

# Active filter summary
active_filters = []
if sel_risks:
    active_filters.append(f"Risk appetite: {', '.join(sel_risks)}")
if sel_regions:
    active_filters.append(f"Region: {', '.join(sel_regions)}")
if sel_accts:
    active_filters.append(f"Product: {', '.join(sel_accts)}")
if active_filters:
    st.sidebar.markdown("---")
    st.sidebar.caption("**Active filters:**\n" + "\n".join(f"- {f}" for f in active_filters))

where = build_where_clause(sel_risks, sel_regions, sel_accts)
and_or_where = "AND" if where else "WHERE"

# --- Query all data needed upfront ---
overview_df = con.execute(f"""
SELECT
    COUNT(*) AS accounts,
    SUM(balance) AS total_balance,
    SUM(loan_amount) AS total_loan_amount,
    CASE WHEN SUM(balance) = 0 THEN NULL
         ELSE SUM(interest_rate * balance) / SUM(balance) END AS weighted_avg_interest_rate,
    AVG(net_flow) AS avg_net_flow,
    SUM(net_flow) AS total_net_flow
FROM {BASE} {where}
""").fetchdf()
overview = overview_df.iloc[0].to_dict() if not overview_df.empty else {}

# Top 10% concentration - exclude negative balances for consistent treatment
wealth_df = con.execute(f"""
WITH base AS (
    SELECT balance FROM {BASE} {where}
    {and_or_where} balance IS NOT NULL AND balance >= 0
),
ranked AS (
    SELECT balance,
        ROW_NUMBER() OVER (ORDER BY balance DESC) AS rn,
        COUNT(*) OVER () AS n,
        SUM(balance) OVER () AS total_balance
    FROM base
),
top10 AS (
    SELECT total_balance,
        SUM(CASE WHEN rn <= CEIL(n * 0.10) THEN balance ELSE 0 END) AS top_10_balance
    FROM ranked GROUP BY total_balance
)
SELECT total_balance, top_10_balance,
    CASE WHEN total_balance = 0 THEN NULL ELSE top_10_balance / total_balance END AS top_10_share
FROM top10
""").fetchdf()
wealth = wealth_df.iloc[0].to_dict() if not wealth_df.empty else {}

# Lorenz / Gini
lorenz_df = con.execute(f"""
WITH base AS (
    SELECT balance FROM {BASE} {where}
    {and_or_where} balance IS NOT NULL AND balance >= 0
),
ordered AS (
    SELECT balance,
        ROW_NUMBER() OVER (ORDER BY balance ASC) AS rn,
        COUNT(*) OVER () AS n,
        SUM(balance) OVER () AS total
    FROM base
),
cum AS (
    SELECT rn, n, total, SUM(balance) OVER (ORDER BY rn) AS cum_balance FROM ordered
)
SELECT rn, n,
    CASE WHEN n = 0 THEN NULL ELSE rn::DOUBLE / n END AS cum_pop_share,
    CASE WHEN total = 0 THEN NULL ELSE cum_balance::DOUBLE / total END AS cum_wealth_share
FROM cum ORDER BY rn
""").fetchdf()

gini = None
curve = None
if not lorenz_df.empty and not lorenz_df["cum_pop_share"].isna().all():
    import numpy as np
    start = pd.DataFrame({"cum_pop_share": [0.0], "cum_wealth_share": [0.0]})
    curve = pd.concat([start, lorenz_df[["cum_pop_share", "cum_wealth_share"]]], ignore_index=True)
    # np.trapezoid in NumPy 2.0+, np.trapz in older versions
    trapz_fn = getattr(np, "trapezoid", None) or getattr(np, "trapz")
    area = float(trapz_fn(curve["cum_wealth_share"], curve["cum_pop_share"]))
    gini = 1 - 2 * area

# Risk segmentation (always full portfolio)
risk_full_df = con.execute(f"""
SELECT risk_tolerance, COUNT(*) AS accounts,
    SUM(balance) AS total_balance,
    SUM(loan_amount) AS total_loan_amount,
    AVG(net_flow) AS avg_net_flow
FROM {BASE} GROUP BY 1
ORDER BY CASE risk_tolerance
    WHEN 'Low' THEN 1
    WHEN 'Medium' THEN 2
    WHEN 'High' THEN 3
    ELSE 4
END
""").fetchdf()

# Deciles
deciles_df = con.execute(f"""
WITH base AS (
    SELECT balance FROM {BASE} {where}
    {and_or_where} balance IS NOT NULL AND balance >= 0
),
deciled AS (
    SELECT balance, NTILE(10) OVER (ORDER BY balance DESC) AS decile FROM base
)
SELECT decile, COUNT(*) AS accounts,
    MIN(balance) AS min_balance, AVG(balance) AS avg_balance,
    MAX(balance) AS max_balance, SUM(balance) AS total_balance
FROM deciled GROUP BY decile ORDER BY decile
""").fetchdf()

# Net flow by account type and region (for growth section)
flow_by_product_df = con.execute(f"""
SELECT account_type, SUM(net_flow) AS total_net_flow, COUNT(*) AS accounts,
    SUM(balance) AS total_balance
FROM {BASE} {where}
GROUP BY 1 ORDER BY total_net_flow DESC
""").fetchdf()

flow_by_region_df = con.execute(f"""
SELECT region, SUM(net_flow) AS total_net_flow, COUNT(*) AS accounts,
    SUM(balance) AS total_balance
FROM {BASE} {where}
GROUP BY 1 ORDER BY total_net_flow DESC
""").fetchdf()

# ============================================================
# PAGE HEADER
# ============================================================
st.title("Fintech Portfolio Dashboard")
st.caption(f"Connected to: {DB_PATH}")

# --- Dynamic narrative summary ---
accounts = int(overview.get("accounts") or 0)
total_balance = overview.get("total_balance") or 0
avg_net_flow = overview.get("avg_net_flow") or 0
total_net_flow = overview.get("total_net_flow") or 0
top_10_share = wealth.get("top_10_share")
flow_direction = "growing" if avg_net_flow > 0 else "shrinking"
concentration_label = gini_label(gini).lower()

narrative_lines = [
    f"The portfolio currently holds <b>{accounts:,} accounts</b> with a combined balance of <b>{fmt_money(total_balance)}</b>.",
    f"On average, accounts are <b>{flow_direction}</b> - net contributions are <b>{fmt_money(avg_net_flow)}</b> per account.",
]
if top_10_share is not None:
    narrative_lines.append(
        f"Balance distribution shows <b>{concentration_label}</b>: "
        f"the top 10% of accounts hold <b>{fmt_pct(top_10_share)}</b> of total assets."
    )
if active_filters:
    narrative_lines.append("<i>You are viewing a filtered subset of the portfolio.</i>")

narrative_html = " ".join(narrative_lines)
st.markdown(
    f"""
    <div style="
        background-color: rgba(37, 99, 235, 0.1);
        border-left: 4px solid #2563eb;
        border-radius: 4px;
        padding: 16px 20px;
        font-size: 0.95rem;
        line-height: 1.6;
        color: inherit;
    ">{narrative_html}</div>
    """,
    unsafe_allow_html=True,
)

st.divider()

# ============================================================
# SECTION 1: PORTFOLIO OVERVIEW
# ============================================================
st.subheader("1. Portfolio Overview")
st.caption("A snapshot of the total portfolio size, lending exposure, and product rates across all selected accounts.")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric(
    "Total accounts",
    f"{accounts:,}",
    help="Number of individual customer accounts in the selected portfolio.",
)
c2.metric(
    "Total balance",
    fmt_money(total_balance),
    help="The combined value held across all accounts - equivalent to assets under management (AUM) for non-loan products.",
)
c3.metric(
    "Total credit exposure",
    fmt_money(overview.get("total_loan_amount")),
    help="The total outstanding loan principal across all accounts with a credit product.",
)
c4.metric(
    "Balance-weighted product rate",
    fmt_pct_from_rate(overview.get("weighted_avg_interest_rate"), 2),
    help="The average interest rate across all accounts, weighted by balance size. For loan accounts this is the borrowing rate; for savings/investment accounts it is the yield. Treat as a directional signal, not a precise rate.",
)
c5.metric(
    "Total net contributions",
    fmt_money(total_net_flow),
    help="Total net money movement across all accounts (deposits minus withdrawals). A positive number means the portfolio is growing from customer contributions.",
)
c6.metric(
    "Avg net contributions",
    fmt_money(avg_net_flow),
    help="Average net contributions per account. Useful for understanding typical account behaviour alongside the portfolio-level total.",
)

st.divider()

# ============================================================
# SECTION 2: SEGMENTS & GROWTH DRIVERS
# ============================================================
st.subheader("2. Segments & Growth Drivers")
st.caption(
    "Which customer groups and product types are driving growth? "
    "Net contributions show where money is flowing in or out. "
    "The risk appetite charts always show the full portfolio - your active filters are highlighted in blue."
)

# Net flow by product and region side by side
g1, g2 = st.columns(2)
with g1:
    st.markdown("**Net contributions by product type**")
    st.caption("Positive = more deposits than withdrawals. Negative = more outflows than inflows.")
    if not flow_by_product_df.empty:
        st.plotly_chart(
            bar_chart(flow_by_product_df, "account_type", "total_net_flow",
                      "Net contributions by product", fmt_fn=fmt_money),
            use_container_width=True,
        )

with g2:
    st.markdown("**Net contributions by region**")
    st.caption("Shows which geographies are growing (positive) or losing assets (negative).")
    if not flow_by_region_df.empty:
        st.plotly_chart(
            bar_chart(flow_by_region_df, "region", "total_net_flow",
                      "Net contributions by region", fmt_fn=fmt_money),
            use_container_width=True,
        )

# Customer risk appetite - full portfolio with filter highlight
st.markdown("**Customer risk appetite breakdown**")
st.caption(
    "Risk appetite reflects how much investment risk each customer chose when setting up their account "
    "(e.g. Conservative, Moderate, Aggressive). This is not a credit risk measure."
)

if not risk_full_df.empty:
    active_risks = set(sel_risks) if sel_risks else set(risk_full_df["risk_tolerance"].tolist())

    if sel_risks:
        st.caption(
            f"Showing full portfolio. **Highlighted in blue:** {', '.join(sorted(sel_risks))}. "
            "Grey bars = the rest of the portfolio. This section always shows all tiers so you can see your selection in context."
        )
    else:
        st.caption("Showing all risk appetite tiers across the full portfolio. Use the sidebar to highlight specific tiers.")

    ra1, ra2 = st.columns(2)
    with ra1:
        st.plotly_chart(
            bar_chart(risk_full_df, "risk_tolerance", "total_balance",
                      "Total balance by risk appetite", active_set=active_risks, fmt_fn=fmt_money),
            use_container_width=True,
        )
    with ra2:
        st.plotly_chart(
            bar_chart(risk_full_df, "risk_tolerance", "accounts",
                      "Number of accounts by risk appetite", active_set=active_risks),
            use_container_width=True,
        )

st.divider()

# ============================================================
# SECTION 3: CONCENTRATION & DEPENDENCY RISK
# ============================================================
st.subheader("3. Concentration & Dependency Risk")
st.caption(
    "How dependent is the portfolio on a small number of large accounts? "
    "High concentration means a few accounts hold most of the value - "
    "this is a risk if those accounts leave or become inactive."
)

# Key concentration metrics
con1, con2, con3 = st.columns(3)
con1.metric(
    "Top 10% of accounts hold",
    fmt_pct(top_10_share),
    help="The share of total balance held by the wealthiest 10% of accounts. Above 60–70% is considered high concentration.",
)
con2.metric(
    "Their combined balance",
    fmt_money(wealth.get("top_10_balance")),
    help="The actual dollar value held by the top 10% of accounts by balance.",
)
con3.metric(
    "Gini coefficient",
    f"{gini:.3f} - {gini_label(gini)}" if gini is not None else "-",
    help=(
        "A measure of balance inequality from 0 to 1. "
        "Below 0.35: relatively even. "
        "0.35–0.55: moderate concentration. "
        "0.55–0.70: high concentration. "
        "Above 0.70: extreme concentration."
    ),
)

st.markdown("---")

# Decile chart + Lorenz side by side
d1, d2 = st.columns(2)

with d1:
    st.markdown("**Balance by account decile**")
    st.caption(
        "Accounts are split into 10 equal groups by balance size. "
        "D1 = the wealthiest 10%, D10 = the smallest 10%. "
        "A large gap between D1 and the rest signals high dependency on top accounts."
    )
    if not deciles_df.empty:
        chart_df = deciles_df.copy()
        chart_df["decile"] = chart_df["decile"].astype(int).map(lambda d: f"D{d}")
        fig = go.Figure(go.Bar(
            x=chart_df["decile"],
            y=chart_df["total_balance"],
            marker_color=["#2563eb" if d == "D1" else "#93c5fd" if d in ["D2", "D3"] else "#d1d5db"
                          for d in chart_df["decile"]],
            text=[fmt_money(v) for v in chart_df["total_balance"]],
            textposition="outside",
        ))
        fig.update_layout(
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
            margin=dict(t=20, b=20, l=20, r=20),
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Show full decile breakdown"):
            deciles_display = deciles_df.copy()
            deciles_display["decile"] = deciles_display["decile"].astype(int).map(lambda d: f"D{d}")
            for col in ["min_balance", "avg_balance", "max_balance", "total_balance"]:
                if col in deciles_display.columns:
                    deciles_display[col] = deciles_display[col].map(fmt_money)
            deciles_display = deciles_display.rename(columns={
                "decile": "Decile (D1 = richest)",
                "min_balance": "Min balance",
                "avg_balance": "Avg balance",
                "max_balance": "Max balance",
                "total_balance": "Total balance",
            })
            st.dataframe(deciles_display, use_container_width=True, hide_index=True)

with d2:
    st.markdown("**Lorenz curve**")
    st.caption(
        "Each point on the curve shows what share of total balance is held by the bottom X% of accounts. "
        "If the curve hugged the diagonal, every account would hold an equal share. "
        "The further the curve bows below the diagonal, the more concentrated the portfolio."
    )
    if curve is not None:
        plot_df = curve.copy()
        # Add the perfect equality diagonal for reference
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=plot_df["cum_pop_share"], y=plot_df["cum_pop_share"],
            mode="lines", name="Perfect equality",
            line=dict(color="#d1d5db", dash="dash"),
        ))
        fig.add_trace(go.Scatter(
            x=plot_df["cum_pop_share"], y=plot_df["cum_wealth_share"],
            mode="lines", name="Actual distribution",
            line=dict(color="#2563eb"),
            fill="tonexty", fillcolor="rgba(37,99,235,0.1)",
        ))
        fig.update_layout(
            showlegend=True,
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="Cumulative share of accounts (poorest → richest)", tickformat=".0%"),
            yaxis=dict(title="Cumulative share of total balance", tickformat=".0%", showgrid=True, gridcolor="#f0f0f0"),
            margin=dict(t=20, b=40, l=40, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough data to compute the Lorenz curve.")