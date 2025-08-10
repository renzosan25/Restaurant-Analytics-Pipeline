{{
    config(
        materialized='table'
    )
}}

with base as (
    SELECT * FROM {{ref('stg_restaurants_data')}}
),

parameters as(

    SELECT
        avg(rating) as S_avg_r,
        54 as m_avg

    FROM base
),

bayesian as (
    SELECT
    b.*,
    ((b.userRatingCount::float / (b.userRatingCount+p.m_avg))*b.rating + (p.m_avg::float/(b.userRatingCount + p.m_avg))*p.S_avg_r) as br_place
    FROM base b, parameters p
)

SELECT * FROM bayesian

