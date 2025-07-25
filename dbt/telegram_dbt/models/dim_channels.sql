{{ config(materialized='table') }}

SELECT DISTINCT
    channel AS channel_name,
    ROW_NUMBER() OVER (ORDER BY channel) AS channel_id
FROM {{ ref('stg_telegram_messages') }}