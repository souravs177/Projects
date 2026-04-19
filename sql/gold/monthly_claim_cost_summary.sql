with aggregated as (
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
        max(last_touched_at) as source_max_last_touched_at
    from silver_member_claims
    group by claim_month, plan_id, member_state
)
select
    claim_month,
    plan_id,
    member_state,
    total_claims,
    distinct_members,
    distinct_providers,
    total_billed_amount,
    total_paid_amount,
    average_paid_amount,
    case
        when total_billed_amount = 0 then 0.0
        else round(total_paid_amount / total_billed_amount, 4)
    end as paid_to_billed_ratio,
    source_max_last_touched_at
from aggregated
order by claim_month, plan_id, member_state;
