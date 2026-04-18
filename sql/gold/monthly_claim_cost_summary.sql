select
    claim_month,
    plan_id,
    member_state,
    count(*) as total_claims,
    count(distinct member_id) as distinct_members,
    count(distinct provider_id) as distinct_providers,
    round(sum(billed_amount), 2) as total_billed_amount,
    round(sum(paid_amount), 2) as total_paid_amount,
    round(avg(paid_amount), 2) as average_paid_amount,
    round(sum(paid_amount) / nullif(sum(billed_amount), 0), 4) as paid_to_billed_ratio
from silver_member_claims
group by claim_month, plan_id, member_state
order by claim_month, plan_id, member_state;
