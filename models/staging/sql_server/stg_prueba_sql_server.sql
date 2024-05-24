
{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server__users') }}
    ),

renamed_casted AS (
    SELECT
          USER_ID
        , UPDATED_AT
        , ADDRESS_ID
        , LAST_NAME
        , CREATED_AT
        , PHONE_NUMBER
        , TOTAL_ORDERS
        , FIRST_NAME
        , EMAIL
        , _FIVETRAN_DELETED
        , date_load
    FROM src_users
    )

SELECT * FROM renamed_casted