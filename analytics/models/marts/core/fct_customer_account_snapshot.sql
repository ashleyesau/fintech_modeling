with src as (
    select *
    from {{ ref('stg_fintech_data') }}
)

select
    -- grain
    customer_id,
    account_id,

    -- measures
    balance,
    deposits,
    withdrawals,
    transfers,
    international_transfers,
    investments,
    loan_amount,
    interest_rate,

    -- degenerate descriptors
    loan_purpose,
    loan_term,
    loan_status,
    transaction_description,

    -- opaque blobs (keep raw for now)
    ts_dates,
    ts_transactions,
    ts_repayments
from src
