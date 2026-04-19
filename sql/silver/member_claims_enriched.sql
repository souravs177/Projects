with cleaned_claims as (
    select
        claim_id,
        member_id,
        provider_id,
        cast(service_date as date) as service_date,
        diagnosis_code,
        procedure_code,
        cast(billed_amount as double) as billed_amount,
        cast(paid_amount as double) as paid_amount,
        upper(trim(claim_status)) as claim_status,
        source_file,
        ingested_at,
        record_source
    from bronze_claims
    where claim_id is not null
      and member_id is not null
      and provider_id is not null
      and paid_amount is not null
      and cast(paid_amount as double) >= 0
),
cleaned_members as (
    select
        member_id,
        full_name,
        cast(date_of_birth as date) as date_of_birth,
        gender,
        initcap(trim(city)) as city,
        upper(trim(state)) as state,
        plan_id,
        ingested_at
    from bronze_members
),
cleaned_providers as (
    select
        provider_id,
        provider_name,
        initcap(trim(specialty)) as specialty,
        npi,
        initcap(trim(city)) as city,
        upper(trim(state)) as state,
        ingested_at
    from bronze_providers
)
select
    c.claim_id,
    c.member_id,
    m.full_name as member_name,
    m.date_of_birth,
    m.gender,
    m.city as member_city,
    m.state as member_state,
    m.plan_id,
    c.provider_id,
    p.provider_name,
    p.specialty as provider_specialty,
    p.npi,
    p.city as provider_city,
    p.state as provider_state,
    c.service_date,
    date_trunc('month', c.service_date) as claim_month,
    c.diagnosis_code,
    c.procedure_code,
    c.claim_status,
    c.billed_amount,
    c.paid_amount,
    round(c.billed_amount - c.paid_amount, 2) as patient_responsibility,
    case
        when c.paid_amount >= 200.00 then true
        else false
    end as is_high_cost_claim,
    c.source_file,
    c.ingested_at,
    greatest(
        coalesce(c.ingested_at, cast('1900-01-01 00:00:00' as timestamp)),
        coalesce(m.ingested_at, cast('1900-01-01 00:00:00' as timestamp)),
        coalesce(p.ingested_at, cast('1900-01-01 00:00:00' as timestamp))
    ) as last_touched_at,
    c.record_source
from cleaned_claims c
left join cleaned_members m
    on c.member_id = m.member_id
left join cleaned_providers p
    on c.provider_id = p.provider_id;
