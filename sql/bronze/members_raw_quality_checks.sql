select
    plan_id,
    count(*) as member_count,
    sum(case when member_id is null then 1 else 0 end) as missing_member_id_count,
    sum(case when full_name is null or trim(full_name) = '' then 1 else 0 end) as missing_full_name_count,
    sum(case when date_of_birth is null then 1 else 0 end) as missing_date_of_birth_count,
    sum(case when state is null or trim(state) = '' then 1 else 0 end) as missing_state_count
from bronze_members
group by plan_id
order by plan_id;
