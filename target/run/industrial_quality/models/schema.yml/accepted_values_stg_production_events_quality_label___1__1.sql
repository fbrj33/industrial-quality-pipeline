
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        quality_label as value_field,
        count(*) as n_records

    from "quality_db"."analytics"."stg_production_events"
    group by quality_label

)

select *
from all_values
where value_field not in (
    '-1','1'
)



  
  
      
    ) dbt_internal_test