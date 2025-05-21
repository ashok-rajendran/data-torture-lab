create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
);

select * from customer_orders;


insert into customer_orders values
(1,100,cast('2022-01-01' as date),2000),
(2,200,cast('2022-01-01' as date),2500),
(3,300,cast('2022-01-01' as date),2100),
(4,100,cast('2022-01-02' as date),2000),
(5,400,cast('2022-01-02' as date),2200),
(6,500,cast('2022-01-02' as date),2700),
(7,100,cast('2022-01-03' as date),3000),
(8,400,cast('2022-01-03' as date),1000),
(9,600,cast('2022-01-03' as date),3000)
;

with customer_first_visit as 
(select 
customer_id,
min(order_date) as first_visit_date 
from customer_orders 
group by customer_id
),
customer_stats as 
(select 
co.*,cfv.first_visit_date,
case when co.order_date = cfv.first_visit_date then 1 else 0 end as new_customer,
case when co.order_date <> cfv.first_visit_date then 1 else 0 end as repeat_customer
from customer_orders co 
inner join customer_first_visit cfv 
on co.customer_id = cfv.customer_id
)
select 
order_date, 
sum(new_customer),
sum(repeat_customer)
from customer_stats 
group by order_date
order by order_date
;