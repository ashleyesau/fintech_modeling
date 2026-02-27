with fct as (
    select balance
    from {{ ref('fct_customer_account_snapshot') }}
),

ranked as (
    select
        balance,
        ntile(10) over (order by balance desc) as balance_decile
    from fct
)

select
    sum(balance) as total_balance,
    sum(case when balance_decile = 1 then balance else 0 end) as top_10_balance,
    sum(case when balance_decile = 1 then balance else 0 end) / nullif(sum(balance), 0) as 
top_10_share
from ranked
