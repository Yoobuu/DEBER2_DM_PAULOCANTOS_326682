{{ config(materialized='view') }}

-- Modelo de staging para los viajes Green Taxi
-- Lee desde BRONZE.GREEN_PARQUET_RAW y convierte tipos

select
    to_timestamp_ntz(v:lpep_pickup_datetime::string)   as pickup_datetime,
    to_timestamp_ntz(v:lpep_dropoff_datetime::string)  as dropoff_datetime,
    v:passenger_count::int                             as passenger_count,
    v:trip_distance::float                             as trip_distance,
    v:RatecodeID::int                                  as rate_code_id,
    v:PULocationID::int                                as pu_location_id,
    v:DOLocationID::int                                as do_location_id,
    v:payment_type::int                                as payment_type_id,
    v:fare_amount::float                               as fare_amount,
    v:tip_amount::float                                as tip_amount,
    v:total_amount::float                              as total_amount,
    'green'                                            as service_type,
    run_id, ingest_ts, src_year, src_month, src_file
from {{ source('bronze', 'GREEN_PARQUET_RAW') }}
