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
    balance_decile,
    count(*) as accounts,
    sum(balance) as total_balance,
    avg(balance) as avg_balance
from ranked
group by 1
order by 1
