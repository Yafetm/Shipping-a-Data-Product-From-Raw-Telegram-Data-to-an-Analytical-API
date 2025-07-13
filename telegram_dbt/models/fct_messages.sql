{{ config(materialized='table') }}

SELECT
    s.message_id,
    s.date::DATE AS date,
    d.channel_id,
    s.text,
    s.has_image
FROM {{ ref('stg_telegram_messages') }} s
JOIN {{ ref('dim_channels') }} d ON s.channel = d.channel_name