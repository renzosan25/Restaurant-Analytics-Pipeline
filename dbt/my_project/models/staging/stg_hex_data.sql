{{
    config(
        materialized='table',
        unique_key='id'
    )
}}

with source as(
    SELECT *
    FROM {{ source( 'dev', 'raw_hex_data') }}
),


delete_duplicates as (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY hex_id order by inserted_at) as rep
    FROM source
)


SELECT
    id,
    hex_id,
    latitude,
    longitude,
    inserted_at

FROM delete_duplicates
WHERE rep=1
