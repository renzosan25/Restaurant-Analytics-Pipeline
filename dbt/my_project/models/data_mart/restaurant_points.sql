{{
    config(
        materialized='table'
    )
}}

SELECT 
    r.id,
    r.place_name,
    r.rating,
    r.userRatingCount,
    r.latitude,
    r.longitude,
    r.hasDelivery,
    r.hex_id,
    h.zone_label,
    h.competitive_rating,
    h.competitive_scaled_rating
FROM {{ ref('stg_restaurants_data') }} r
LEFT JOIN {{ ref('restaurants_competitiveness') }} h
    ON r.hex_id = h.hex_id

