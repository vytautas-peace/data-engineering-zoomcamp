with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num, 
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id, 
        SR_Flag, 
        Affiliated_base_number as affiliated_base_number
    from source
    -- Filter out records with null dispatching_base_num (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
