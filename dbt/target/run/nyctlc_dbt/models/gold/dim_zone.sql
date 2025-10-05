
  
    

        create or replace transient table DM_NYCTLC.SILVER_GOLD.dim_zone
         as
        (

-- Dimensión de zonas de taxi
-- Cada zona tiene un surrogate key único (zone_sk)

select
    dense_rank() over (order by location_id) as zone_sk,
    location_id,
    borough,
    zone_name
from DM_NYCTLC.SILVER_SILVER.stg_taxi_zones
        );
      
  