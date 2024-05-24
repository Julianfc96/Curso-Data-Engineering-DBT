
{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          PROMO_ID
        , DISCOUNT
        , STATUS
        , _FIVETRAN_DELETED
        , _FIVETRAN_SYNCED AS date_load
    FROM src_promos
    )

SELECT * FROM renamed_casted