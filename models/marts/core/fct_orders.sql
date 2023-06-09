with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

order_payments as (
    select
        order_id
        , sum(
            case 
            when payment_status = 'success' then payment_amount 
            end
            ) as amount
    from payments
    group by order_id
),

final as (

    select
        orders.order_id
        , orders.customer_id
        , orders.order_date
        , coalesce(order_payments.amount, 0) as amount

    from orders

    left join order_payments ON orders.order_id = order_payments.order_id

)

select * from final