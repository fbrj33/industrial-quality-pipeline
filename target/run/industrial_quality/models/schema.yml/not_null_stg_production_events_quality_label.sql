
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quality_label
from "quality_db"."analytics"."stg_production_events"
where quality_label is null



  
  
      
    ) dbt_internal_test