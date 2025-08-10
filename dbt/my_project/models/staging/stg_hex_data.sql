{{
    config(
        materialized='table',
        unique_key='hex_id'
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
    hex_contour,
    inserted_at

FROM delete_duplicates
WHERE rep=1
