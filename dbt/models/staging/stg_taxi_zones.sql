{{ config(materialized='view') }}

-- Modelo de staging para Taxi Zones
-- Convierte los tipos base de la tabla BRONZE.TAXI_ZONES

select
    locationid::int      as location_id,
    borough,
    zone as zone_name
from {{ source('bronze', 'TAXI_ZONES_RAW') }}
