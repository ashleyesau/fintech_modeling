with src as (
    select *
    from {{ ref('stg_fintech_data') }}
)

select
    account_id,
    account_type,
    transaction_threshold
from src
