with

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

order_totals as (

    select

        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars

    from payments
    group by 1, 2

),

order_values_joined as (

    select

        orders.*,
        orders.user_id as customer_id,
        order_totals.payment_status,
        order_totals.order_value_dollars

    from orders
    left join order_totals
        on orders.id = order_totals.order_id

)

select * from order_values_joined