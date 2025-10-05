
    
    

with child as (
    select pu_zone_sk as from_field
    from DM_NYCTLC.SILVER_GOLD.fct_trips
    where pu_zone_sk is not null
),

parent as (
    select zone_sk as to_field
    from DM_NYCTLC.SILVER_GOLD.dim_zone
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


