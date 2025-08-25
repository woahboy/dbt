with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

---
customer_orders as (

    select

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        --- Customer level aggregations
        min(order_date) over(
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) order_count,

        sum(nvl2(orders.valid_order_date, 1, 0)) over(
            partition by orders.customer_id
        ) as customer_non_returned_order_count,

        sum(nvl2(
            orders.valid_order_date,
            orders.order_value_dollars,
            0 
        )) over (
            partition by orders.customer_id
        ) as customer_total_lifetime_value,

        array_agg(distinct orders.id) as order_ids

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id

),

add_avg_order_values as (

    select 
    
    *,
    customer_total_lifetime_value / customer_non_returned_order_count as avg_non_returned_order_value

    from customer_orders

),

final as (

select 

    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    customer_first_order_date as first_order_date,
    order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    orders.order_value_dollars,
    orders.status as order_status,
    orders.payment_status

from orders

join customers
on orders.user_id = customers.customer_id

join customer_orders
on orders.user_id = customer_orders.customer_id

)

select * from final