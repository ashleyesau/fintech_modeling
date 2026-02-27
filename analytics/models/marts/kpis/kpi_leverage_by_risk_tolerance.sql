with spine as (
    select *
    from {{ ref('int_customer_account_metrics') }}
),

ranked as (
    select
        *,
        ntile(4) over (order by loan_to_income asc) as leverage_quartile
    from spine
)

select
    risk_tolerance,
    count(*) as accounts,

    avg(income_level) as avg_income,
    avg(loan_amount) as avg_loan_amount,

    avg(loan_to_income) as avg_loan_to_income,

    -- % of accounts in highest leverage quartile (Q4)
    avg(case when leverage_quartile = 4 then 1 else 0 end) as pct_top_quartile_leverage

from ranked
group by 1
order by 1
