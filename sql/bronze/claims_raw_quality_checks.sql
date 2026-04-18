select
    claim_status,
    count(*) as claim_count,
    sum(case when claim_id is null then 1 else 0 end) as missing_claim_id_count,
    sum(case when member_id is null then 1 else 0 end) as missing_member_id_count,
    sum(case when provider_id is null then 1 else 0 end) as missing_provider_id_count,
    sum(case when paid_amount < 0 then 1 else 0 end) as negative_paid_amount_count
from bronze_claims
group by claim_status
order by claim_status;
