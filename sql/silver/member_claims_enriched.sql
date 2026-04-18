select
    c.claim_id,
    c.member_id,
    m.full_name as member_name,
    m.plan_id,
    m.state as member_state,
    c.provider_id,
    p.provider_name,
    p.specialty as provider_specialty,
    p.state as provider_state,
    cast(c.service_date as date) as service_date,
    date_trunc('month', cast(c.service_date as date)) as claim_month,
    upper(c.claim_status) as claim_status,
    cast(c.billed_amount as double) as billed_amount,
    cast(c.paid_amount as double) as paid_amount,
    cast(c.billed_amount as double) - cast(c.paid_amount as double) as patient_responsibility
from bronze_claims c
left join bronze_members m
    on c.member_id = m.member_id
left join bronze_providers p
    on c.provider_id = p.provider_id
where c.claim_id is not null
  and c.member_id is not null
  and c.provider_id is not null
  and cast(c.paid_amount as double) >= 0;
