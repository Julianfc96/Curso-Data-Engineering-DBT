
WITH events AS (
    SELECT * 
    FROM {{ref('base_sql_server__events')}}
),

events_sum AS (
    SELECT *
    FROM {{ref('int_events_sum')}}

),

joined AS (
    SELECT DISTINCT
        a.user_id,
        {{dbt_utils.generate_surrogate_key(['MIN(a.created_at) OVER (PARTITION BY a.user_id)::date'])}} AS first_event,
        {{dbt_utils.generate_surrogate_key(['MAX(a.created_at) OVER (PARTITION BY a.user_id)::date'])}} AS last_event,
        COUNT(DISTINCT a.session_id)OVER(PARTITION BY a.user_id) AS sessions,
        --DATEDIFF(minute, MAX(first_event), MIN(first_event)) AS DateDiff,
        b.checkout_amount,
        b.package_shipped_amount,
        b.add_to_cart_amount,
        b.page_view_amount
    FROM events a 
    LEFT JOIN events_sum b
    ON a.user_id = b.user_id
    ORDER BY sessions DESC
)

SELECT * FROM joined