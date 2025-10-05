
  
    

        create or replace transient table DM_NYCTLC.SILVER_GOLD.fct_trips
         as
        (

-- Tabla de hechos principal: fct_trips
-- Granularidad: 1 fila = 1 viaje

select
    s.trip_hash,
    s.pickup_datetime,
    s.dropoff_datetime,
    s.service_type,
    s.passenger_count,
    s.trip_distance,
    s.fare_amount,
    s.tip_amount,
    s.total_amount,
    -- Claves foráneas hacia dimensiones
    dz1.zone_sk as pu_zone_sk,
    dz2.zone_sk as do_zone_sk,
    s.pickup_borough,
    s.dropoff_borough,
    s.pickup_zone,
    s.dropoff_zone,
    s.rate_code_id,
    s.payment_type_id
from DM_NYCTLC.SILVER_SILVER.silver_trips_unified s
left join DM_NYCTLC.SILVER_GOLD.dim_zone dz1 on dz1.location_id = s.pu_location_id
left join DM_NYCTLC.SILVER_GOLD.dim_zone dz2 on dz2.location_id = s.do_location_id
        );
      
  