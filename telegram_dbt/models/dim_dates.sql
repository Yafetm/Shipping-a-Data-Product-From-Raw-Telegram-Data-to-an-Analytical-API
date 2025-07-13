{{ config(materialized='table') }}

SELECT DISTINCT
    date::DATE AS date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(DOW FROM date) AS day_of_week
FROM {{ ref('stg_telegram_messages') }}