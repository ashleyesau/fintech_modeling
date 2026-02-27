with src as (
    select *
    from {{ ref('stg_fintech_data') }}
)

select
    customer_id,
    age,
    occupation,
    risk_tolerance,
    investment_goals,
    education_level,
    marital_status,
    dependents,
    region,
    sector,
    income_level,
    employment_history,
    employment_status,
    financial_history
from src
