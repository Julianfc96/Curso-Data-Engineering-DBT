
{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server', 'orders') }}
    ),

renamed_casted AS (
    SELECT
          ORDER_ID
        , SHIPPING_SERVICE
        , SHIPPING_COST
        , ADDRESS_ID
        , CREATED_AT
        , PROMO_ID
        , ESTIMATED_DELIVERY_AT
        , ORDER_COST
        , USER_ID
        , ORDER_TOTAL
        , DELIVERED_AT
        , TRACKING_ID
        , STATUS
        , _FIVETRAN_DELETED
        , _FIVETRAN_SYNCED AS date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted