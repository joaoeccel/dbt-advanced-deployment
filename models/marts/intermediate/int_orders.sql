with

orders as (

  select * from {{ ref('stg_orders') }}
  
),

-- CTE to GET data from stg_payments
payments_get_data as (

  select * from {{ ref('stg_payments') }}

), 

-- CTE to FILTER data from stg_payments
payments as (

  select * from payments_get_data
  where payments_get_data.payment_status != 'fail'

), 


order_totals as (

  select
    
    order_id
    , payment_status
    , sum(payment_amount) as order_value_dollars

  from payments
  group by 1, 2

),

order_values_joined as (

  select
    
    orders.*
    , order_totals.payment_status
    , order_totals.order_value_dollars

  from orders
  left join order_totals 
    on orders.order_id = order_totals.order_id

)

select * from order_values_joined
