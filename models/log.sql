select 
distinct event_name as tipo,
event_schema as schema,
event_model as modelo,
event_user as usuario,
event_target as event_target
from {{target.schema}}_meta.dbt_audit_log