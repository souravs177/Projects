select
    claim_month,
    provider_specialty,
    provider_state,
    count(*) as total_claims,
    count(distinct provider_id) as distinct_providers,
    count(distinct member_id) as distinct_members,
    round(sum(paid_amount), 2) as total_paid_amount,
    round(avg(patient_responsibility), 2) as average_patient_responsibility,
    sum(case when is_high_cost_claim then 1 else 0 end) as high_cost_claim_count,
    max(last_touched_at) as source_max_last_touched_at
from silver_member_claims
group by claim_month, provider_specialty, provider_state
order by claim_month, provider_specialty, provider_state;
