{{ config(
    materialized='incremental',
    unique_key='trip_hash',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}


with all_trips as (
    select * from {{ ref('stg_trips_yellow') }}
    union all
    select * from {{ ref('stg_trips_green') }}
),

with_zones as (
    select
        t.*,
        zpu.zone_name   as pickup_zone,
        zpu.borough     as pickup_borough,
        zdo.zone_name   as dropoff_zone,
        zdo.borough     as dropoff_borough
    from all_trips t
    left join {{ ref('stg_taxi_zones') }} zpu on t.pu_location_id = zpu.location_id
    left join {{ ref('stg_taxi_zones') }} zdo on t.do_location_id = zdo.location_id
),

with_hash as (
    select
        *,
        md5(concat_ws('|',
            coalesce(to_varchar(pickup_datetime),''),
            coalesce(to_varchar(dropoff_datetime),''),
            coalesce(to_varchar(pu_location_id),''),
            coalesce(to_varchar(do_location_id),''),
            coalesce(service_type,'')
        )) as trip_hash
    from with_zones
    where 1=1
      
      and pickup_datetime  between '2009-01-01'::timestamp_ntz and '2025-12-31'::timestamp_ntz
      and dropoff_datetime between '2009-01-01'::timestamp_ntz and '2025-12-31'::timestamp_ntz
    {% if is_incremental() %}
      
      and ingest_ts >= (
        select coalesce(max(ingest_ts), '1900-01-01'::timestamp_ntz) from {{ this }}
      )
    {% endif %}
),

dedup as (
    select
        *,
        row_number() over (partition by trip_hash order by ingest_ts desc) as rn
    from with_hash
)

select
    /* columnas finales */
    trip_hash,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    pu_location_id,
    do_location_id,
    payment_type_id,
    fare_amount,
    tip_amount,
    total_amount,
    service_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    run_id, ingest_ts, src_year, src_month, src_file
from dedup
where rn = 1
