WITH src_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server__events') }}
    ),

type AS (
    SELECT DISTINCT
     md5(event_type) AS event_type_id,
     event_type
     FROM src_events
)

SELECT * FROM type