select * from nw.customers c 

select * from nw.orders o

select customer_id , count(*) from nw.orders o group by customer_id having count(*) > 1

select employee_id , count(*) from nw.orders o group by employee_id having count(*) > 1

select * from nw.employees e 

select * from nw.order_items oi 

select * from nw.products p 
  
select * from nw.categories c


select date_trunc('day', order_date)::date as day, sum(oi.amount) as sum_amount, count(distinct o.order_id) as daily_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('day', order_date)::date 
order by 1 

select date_trunc('week', order_date)::date as week, sum(oi.amount) as sum_amount, count(distinct o.order_id) as weekly_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('week', order_date)::date 
order by 1 

select date_trunc('month', order_date)::date as month, sum(oi.amount) as sum_amount, count(distinct o.order_id) as monthly_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('month', order_date)::date 
order by 1 

select date_trunc('quarter', order_date)::date as quarter, sum(oi.amount) as sum_amount, count(distinct o.order_id) as quarterly_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('quarter', order_date)::date 
order by 1 

select date_trunc('year', order_date)::date as year, sum(oi.amount) as sum_amount, count(distinct o.order_id) as yearly_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('year', order_date)::date 
order by 1 

select date_trunc('day', order_date)::date as day, oi.product_id  ,sum(oi.amount) as sum_amount, count(distinct o.order_id) as daily_ord_cnt
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('day', order_date)::date ,oi.product_id 
order by 1,2


with 
temp_01 as (
select c.category_name, to_char(date_trunc('month', o.order_date), 'yyyymm') as month, sum(amount) as sum_amount, count(distinct o.order_id) as monthly_order_count
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
	join nw.products p on oi.product_id = p.product_id 
	join nw.categories c on p.category_id = c.category_id 
group by c.category_name, to_char(date_trunc('month', o.order_date), 'yyyymm')  
)
select *
	,sum(t.sum_amount) over (partition by month) as month_total_amount
	,round(t.sum_amount / sum(sum_amount) over (partition by month), 3) as month_ratio
from temp_01 t

with
temp_01 as (
select p.product_id, max(p.product_name) as product_name ,max(c.category_name) as category_name ,sum(oi.amount) as sum_amount
from nw.order_items oi
	join nw.products p on oi.product_id = p.product_id
	join nw.categories c on p.category_id = c.category_id 
group by p.product_id 
)
select t.product_name, t.sum_amount as product_sales, t.category_name
	,sum(sum_amount) over (partition by t.category_name)
	,round(t.sum_amount / sum(sum_amount) over (partition by t.category_name), 3) as product_category_ratio
	,row_number() over (partition by t.category_name order by t.sum_amount desc) as product_sales_rank
from temp_01 t
order by t.category_name, product_sales desc

with 
temp_01 as (
select date_trunc('month', o.order_date)::date as month, sum(oi.amount) as sum_amount
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by date_trunc('month', o.order_date)::date 
)
select *
	, date_trunc('year', month)::date as year
	, date_trunc('quarter', month)::date as quarter
	, sum(sum_amount) over (partition by date_trunc('year', month)::date order by month) as cum_year_amount
	, sum(sum_amount) over (partition by date_trunc('quarter', month)::date order by month) as cum_quarter_amount
from temp_01

with 
temp_01 as (
select date_trunc('day', o.order_date)::date as day, sum(oi.amount) as sum_amount
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
where o.order_date >= to_date('1996-07-08', 'yyyy-mm-dd') 
group by date_trunc('day', o.order_date)::date 
),
temp_02 as (
select day, sum_amount
	, avg (sum_amount) over (order by day rows between 4 preceding and current row) as moving_avg_5days
	, row_number () over (order by day) as row_num
from temp_01  
)
select *
	, case when row_num < 5 then null
		else moving_avg_5days end as moving_avg_5days
from temp_02

with 
temp_01 as (
select date_trunc('day', o.order_date)::date as day
	, sum(oi.amount) as sum_amount
	, row_number() over (order by date_trunc('day', o.order_date)::date) as row_num
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
where o.order_date >= to_date('1996-07-08', 'yyyy-mm-dd') 
group by date_trunc('day', o.order_date)::date 
),
temp_02 as (
select ta.day, ta.sum_amount, ta.row_num, tb.day as day_back, tb.sum_amount as sum_amount_back, tb.row_num as row_num_back
from temp_01 ta
	join temp_01 tb on ta.row_num between tb.row_num and tb.row_num + 4
)
select day
	, avg(sum_amount_back) as moving_avg_5days
	, avg(sum_amount_back) / 5 as moving_avg_5days_01
	, sum(case when row_num - row_num_back = 4 then 0.5 * sum_amount_back
			   when row_num - row_num_back in (3, 2, 1) then sum_amount_back
			   when row_num - row_num_back = 0 then 1.5 * sum_amount_back
		end) as moving_weighted_sum
	, sum(case when row_num - row_num_back = 4 then 0.5 * sum_amount_back
			   when row_num - row_num_back in (3, 2, 1) then sum_amount_back
			   when row_num - row_num_back = 0 then 1.5 * sum_amount_back
		end) / 5 as moving_weighted_avg_sum
from temp_02
group by day
having count(*) = 5
order by 1
