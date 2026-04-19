select
    specialty,
    count(*) as provider_count,
    sum(case when provider_id is null then 1 else 0 end) as missing_provider_id_count,
    sum(case when provider_name is null or trim(provider_name) = '' then 1 else 0 end) as missing_provider_name_count,
    sum(case when npi is null or trim(npi) = '' then 1 else 0 end) as missing_npi_count,
    sum(case when state is null or trim(state) = '' then 1 else 0 end) as missing_state_count
from bronze_providers
group by specialty
order by specialty;
