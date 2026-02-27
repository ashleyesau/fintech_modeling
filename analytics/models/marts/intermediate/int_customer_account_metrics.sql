with fct as (
    select *
    from {{ ref('fct_customer_account_snapshot') }}
),
cust as (
    select customer_id, risk_tolerance, region, income_level
    from {{ ref('dim_customer') }}
),
acct as (
    select account_id, account_type
    from {{ ref('dim_account') }}
)

select
    fct.customer_id,
    fct.account_id,

    -- sliceable descriptors
    cust.risk_tolerance,
    cust.region,
    acct.account_type,

    -- base measures (kept for convenience)
    fct.balance,
    fct.deposits,
    fct.withdrawals,
    fct.investments,
    fct.loan_amount,
    fct.interest_rate,
    cust.income_level,

    -- derived (non-aggregated) metrics
    (fct.deposits - fct.withdrawals) as net_flow,
    (fct.loan_amount / nullif(cust.income_level, 0)) as loan_to_income,
    (fct.loan_amount / nullif(fct.balance, 0)) as loan_to_balance,
    (fct.investments / nullif(fct.balance, 0)) as investment_to_balance

from fct
join cust on fct.customer_id = cust.customer_id
join acct on fct.account_id = acct.account_id
