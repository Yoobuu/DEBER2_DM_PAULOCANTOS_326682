select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select trip_hash
from DM_NYCTLC.SILVER_GOLD.fct_trips
where trip_hash is null



      
    ) dbt_internal_test