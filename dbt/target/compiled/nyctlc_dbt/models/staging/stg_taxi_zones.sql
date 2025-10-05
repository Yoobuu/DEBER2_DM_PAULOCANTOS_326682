

-- Modelo de staging para Taxi Zones
-- Convierte los tipos base de la tabla BRONZE.TAXI_ZONES

select
    locationid::int      as location_id,
    borough,
    zone as zone_name
from DM_NYCTLC.BRONZE.TAXI_ZONES_RAW