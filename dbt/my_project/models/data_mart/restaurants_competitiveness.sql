{{
    config(
        materialized='table'
    )
}}

SELECT
    r.hex_id,
    COUNT(*) densidad,
    ROUND(AVG(r.rating)::numeric,2) as avg_rating,
    h.latitude,
    h.longitude
    
FROM {{ ref('stg_restaurants_data') }} r
LEFT JOIN {{ ref('stg_hex_data')}} h 
    ON r.hex_id = h.hex_id

GROUP BY r.hex_id, h.latitude, h.longitude
ORDER BY avg_rating DESC





