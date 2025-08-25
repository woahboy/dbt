with 

source as (

    select * from {{ source("jaffle_shop", "orders")}}

),

transformed as (

    select 

        id as order_id,

        (case 
            when source.status not in ('returned','return_pending') 
            then order_date 
        end) as valid_order_date,

        row_number() over (
                partition by user_id 
                order by order_date, id
            ) as user_order_seq,
        *

      from source

)

select * from transformed