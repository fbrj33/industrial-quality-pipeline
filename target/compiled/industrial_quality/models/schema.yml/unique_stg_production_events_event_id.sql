
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "quality_db"."analytics"."stg_production_events"
where event_id is not null
group by event_id
having count(*) > 1


