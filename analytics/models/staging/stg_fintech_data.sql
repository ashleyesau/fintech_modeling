with source as (
    select * from {{ source('raw', 'fintech_data') }}
)

select
  "Transaction Description" as transaction_description,

  "Customer Profile.customer_id" as customer_id,
  "Customer Profile.age" as age,
  "Customer Profile.occupation" as occupation,
  "Customer Profile.risk_tolerance" as risk_tolerance,
  "Customer Profile.investment_goals" as investment_goals,
  "Customer Profile.education_level" as education_level,
  "Customer Profile.marital_status" as marital_status,
  "Customer Profile.dependents" as dependents,
  "Customer Profile.region" as region,
  "Customer Profile.financial_history" as financial_history,
  "Customer Profile.sector" as sector,
  "Customer Profile.income_level" as income_level,
  "Customer Profile.employment_history" as employment_history,
  "Customer Profile.address" as address,

  -- Account Activity.customer_id is redundant (0 mismatches vs customer_id)
  -- so we drop it

  "Account Activity.account_id" as account_id,
  "Account Activity.balance" as balance,
  "Account Activity.deposits" as deposits,
  "Account Activity.withdrawals" as withdrawals,
  "Account Activity.transfers" as transfers,
  "Account Activity.international_transfers" as international_transfers,
  "Account Activity.investments" as investments,
  "Account Activity.account_type" as account_type,
  "Account Activity.transaction_threshold" as transaction_threshold,

  "Loan Application Summary.loan_amount" as loan_amount,
  "Loan Application Summary.loan_purpose" as loan_purpose,
  "Loan Application Summary.employment_status" as employment_status,
  "Loan Application Summary.loan_term" as loan_term,
  "Loan Application Summary.interest_rate" as interest_rate,
  "Loan Application Summary.loan_status" as loan_status,

  "Time Series Data.dates" as ts_dates,
  "Time Series Data.transactions" as ts_transactions,
  "Time Series Data.repayments" as ts_repayments
from source
