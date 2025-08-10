{{
    config(
        materialized='table'
    )
}}

with base as(
    SELECT * FROM {{ref('bayes_metrics')}}
),

agg AS (
    
SELECT
    hex_id, 
    COUNT(*) as count_places,
    sum(userRatingCount) as sum_user_ratings,
    avg(userRatingCount::float) as avg_votes_place,
    avg(rating) as avg_rating,
    avg(br_place) as bayesian_avg_rating,
    percentile_cont(0.75) within group(order by rating) - percentile_cont(0.25) within group (order by rating) as iqr_rating,
    avg(hasDelivery::int) as share_delivery
FROM base
GROUP BY hex_id 
),

params AS (
    SELECT
        avg(share_delivery) as S_sh,
        11 AS m_avg
    FROM agg
)

SELECT
    a.*,
    ((a.count_places::float / (a.count_places + p.m_avg))*a.share_delivery + (p.m_avg::float/(a.count_places + p.m_avg))*p.S_sh) as bayesian_share_delivery

FROM agg a
CROSS JOIN params p
