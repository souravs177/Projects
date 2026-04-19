select
    claim_month,
    plan_id,
    claim_status,
    count(*) as claim_count,
    round(sum(billed_amount), 2) as total_billed_amount,
    round(sum(paid_amount), 2) as total_paid_amount,
    round(avg(patient_responsibility), 2) as average_patient_responsibility,
    max(last_touched_at) as source_max_last_touched_at
from silver_member_claims
group by claim_month, plan_id, claim_status
order by claim_month, plan_id, claim_status;
