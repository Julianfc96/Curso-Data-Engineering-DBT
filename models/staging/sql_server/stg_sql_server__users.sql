with
    src_users as (select * from {{ source("sql_server_dbo", "users") }}),

    user_orders as (
        select user_id, count(*) as total_orders
        from {{ ref("base_sql_server__orders") }}
        group by user_id
    ),

    renamed_casted as (
        select
            a.user_id,
            first_name,
            last_name,
            phone_number,
            email,
            coalesce(b.total_orders, 0) as total_orders,
            address_id,
            -- coalesce (regexp_like(email,
            -- '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')= true,false) as
            -- is_valid_email_address,
            convert_timezone('UTC', updated_at)::date as updated_at_utc,
            convert_timezone('UTC', created_at)::date as created_at_utc,
            coalesce(_fivetran_deleted, false) as date_deleted,
            convert_timezone('UTC', _fivetran_synced) as date_load
        from src_users a
        left join user_orders b on a.user_id = b.user_id
    )

select *
from renamed_casted
