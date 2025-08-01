{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

with source as(
    SELECT *
    FROM {{ source( 'dev', 'raw_restaurants_data') }}
),



delete_duplicates as (

    SELECT *, ROW_NUMBER() OVER (PARTITION BY id_location order by inserted_at) as rep
    FROM source
)


SELECT
    id,
    id_location,
    latitude,
    longitude,
    rating,
    userRatingCount,
    place_name,
    hasDelivery,
    hex_id,
    hex_consult,
    inserted_at

FROM delete_duplicates
WHERE rep=1
