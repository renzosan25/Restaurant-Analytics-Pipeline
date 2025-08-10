{{
    config(
        materialized='table'
    )
}}

with row_location as (
    SELECT
    *,
    ROW_NUMBER() over (order by hex_id) as hex_index
    FROM {{ref('hex_kpis')}}
),

assign_location as(
    
    SELECT
    *,
    'Zone_' || lpad(hex_index::text, 3, '0') as zone_label
    FROM row_location
),

base as(
    SELECT *,
        ln(1 + sum_user_ratings) as log_votes
    FROM assign_location

),

variables as (
    SELECT
        avg(bayesian_avg_rating) as mu_br,
        stddev_pop(bayesian_avg_rating) as sd_br,
        avg(log_votes) as mu_lv,
        stddev_pop(log_votes) as sd_lv,
        avg(count_places) as mu_cp,
        stddev_pop(count_places) as sd_cp,
        avg(avg_votes_place) as mu_avp,
        stddev_pop(avg_votes_place) as sd_avp,
        avg(bayesian_share_delivery) as mu_shd,
        stddev_pop(bayesian_share_delivery) as sd_shd
    FROM base
),

z_values AS(
    
    SELECT
        b.*,
        (b.bayesian_avg_rating - v.mu_br) / NULLIF(v.sd_br, 0) AS z_br,
        (b.log_votes - v.mu_lv) / NULLIF(v.sd_lv, 0) AS z_lv,
        (b.count_places - v.mu_cp) / NULLIF(v.sd_cp, 0) AS z_cp,
        (b.avg_votes_place - v.mu_avp) / NULLIF(v.sd_avp, 0) AS z_avp,
        (b.bayesian_share_delivery - v.mu_shd) / NULLIF(v.sd_shd, 0) AS z_shd

    FROM base b
    CROSS JOIN variables v
),


statistic_rating as(

    SELECT
        z.*,
        CASE
            WHEN
                ((CASE WHEN z.z_br IS NOT NULL THEN 30 ELSE 0 END) +
                (CASE WHEN z.z_lv IS NOT NULL THEN 25 ELSE 0 END) +
                (CASE WHEN z.z_cp IS NOT NULL THEN 25 ELSE 0 END) +
                (CASE WHEN z.z_avp IS NOT NULL THEN 10 ELSE 0 END) +
                (CASE WHEN z.z_shd IS NOT NULL THEN 10 ELSE 0 END)) = 0  
            THEN NULL
            ELSE
                CASE
                    WHEN
                    (
                        30 * COALESCE(z.z_br, 0) +
                        25 * COALESCE(z.z_lv, 0) +
                        25 * COALESCE(z.z_cp, 0) +
                        10 * COALESCE(z.z_avp, 0) +
                        10 * COALESCE(z.z_shd, 0)
                    ) < 0
                    THEN(
                        (
                            30 * COALESCE(z.z_br, 0) +
                            25 * COALESCE(z.z_lv, 0) +
                            25 * COALESCE(z.z_cp, 0) +
                            10 * COALESCE(z.z_avp, 0) +
                            10 * COALESCE(z.z_shd, 0)
                        ) * 100.0/ 
                        (
                            (CASE WHEN z.z_br IS NOT NULL THEN 30 ELSE 0 END) +
                            (CASE WHEN z.z_lv IS NOT NULL THEN 25 ELSE 0 END) +
                            (CASE WHEN z.z_cp IS NOT NULL THEN 25 ELSE 0 END) +
                            (CASE WHEN z.z_avp IS NOT NULL THEN 10 ELSE 0 END) +
                            (CASE WHEN z.z_shd IS NOT NULL THEN 10 ELSE 0 END)  
                        )
                    )
                    ELSE (
                            30 * COALESCE(z.z_br, 0) +
                            25 * COALESCE(z.z_lv, 0) +
                            25 * COALESCE(z.z_cp, 0) +
                            10 * COALESCE(z.z_avp, 0) +
                            10 * COALESCE(z.z_shd, 0)
                    )
                END
        END AS competitive_rating
        -- 30: bayesian_avg_rating, calidad promedio ajustada por la media bayesiana (confiabilidad),
        -- 25: log_votes, popularidad por zona suavizada con logaritmo
        -- 25: count_places, densidad por zona (locales por hexágono)
        -- 10: avg_votes_place, popularidad promedio por local (interacción media de los usuarios)
        -- 10: bayesian_share_delivery, penetración del canal de delivery(ventaja competitiva)
    FROM z_values as z

),

bounds as (
    SELECT
    MIN(competitive_rating) as min_rating,
    MAX(competitive_rating) as max_rating
    FROM statistic_rating
),

statistic_scaled_rating as(
    SELECT
        s.*,
        CASE
            WHEN (b.max_rating - b.min_rating) = 0 THEN 0
            ELSE ROUND(
                (100.0 * (s.competitive_rating - b.min_rating) / (b.max_rating - b.min_rating))::numeric,2
            )
        END AS competitive_scaled_rating
    FROM statistic_rating s
    CROSS JOIN bounds b
)

SELECT
    s.*,
    h.hex_contour
FROM statistic_scaled_rating s
INNER JOIN {{ref('stg_hex_data')}} h
ON s.hex_id = h.hex_id

