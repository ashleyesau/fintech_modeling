with fct as (
    select *
    from {{ ref('fct_customer_account_snapshot') }}
),
cust as (
    select *
    from {{ ref('dim_customer') }}
)

select
    cust.risk_tolerance,

    count(*) as accounts,

    sum(fct.balance) as total_balance,
    avg(fct.balance) as avg_balance,

    sum(fct.loan_amount) as total_loan_amount,

    avg(fct.deposits - fct.withdrawals) as avg_net_flow,

    -- weighted average interest rate (weighted by loan exposure)
    sum(fct.loan_amount * fct.interest_rate) / nullif(sum(fct.loan_amount), 0) as 
weighted_avg_interest_rate

from fct
join cust
  on fct.customer_id = cust.customer_id
group by 1
order by 1
