{{ config(materialized='view') }}

SELECT
    message_id,
    date,
    -- Clean text by removing non-ASCII characters if needed
    REGEXP_REPLACE(text, '[^\x00-\x7F]', '') AS text,
    has_image,
    channel
FROM {{ source('raw', 'telegram_messages') }}