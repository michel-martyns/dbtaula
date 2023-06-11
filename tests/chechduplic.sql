select 
count(*) as count,
company_name,
contact_name
from {{source('sources','customers')}}
--{{ref('customers')}}
group by company_name, contact_name
having count > 1