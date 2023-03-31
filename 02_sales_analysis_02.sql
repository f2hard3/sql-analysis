with 
temp_01 as (
select date_trunc('month', order_date)::date as month_date
	, sum(amount) as sum_amount
from nw.orders o 
	join nw.order_items oi on o.order_id  = oi.order_id 
group by date_trunc('month', order_date)::date 
),
temp_02 as (
select month_date, sum_amount as curr_amount
	, lag(month_date, 12) over (order by month_date) as prev_year_month
	, lag(sum_amount, 12) over (order by month_date) as prev_year_amount
from temp_01
)
select *
	, t2.curr_amount - t2.prev_year_amount as diff_amount
	, 100.0 * t2.curr_amount / t2.prev_year_amount as curr_by_prev_amount
	, 100.0 * (t2.curr_amount - t2.prev_year_amount) / t2.prev_year_amount as growth_from_prev
from temp_02 t2
where prev_year_month is not null

with 
temp_01 as (
select date_trunc('quarter', order_date)::date as quarter_date
	, sum(amount) as sum_amount
from nw.orders o 
	join nw.order_items oi on o.order_id  = oi.order_id 
group by date_trunc('quarter', order_date)::date 
),
temp_02 as (
select quarter_date, sum_amount as curr_amount
	, lag(quarter_date, 4) over (order by quarter_date) as prev_year_quarter 
	, lag(sum_amount, 4) over (order by quarter_date) as prev_quarter_amount
from temp_01
)
select *
	, t2.curr_amount - t2.prev_quarter_amount as diff_amount
	, 100.0 * t2.curr_amount / t2.prev_quarter_amount as curr_by_prev_amount
	, 100.0 * (t2.curr_amount - t2.prev_quarter_amount) / t2.prev_quarter_amount as growth_from_prev
from temp_02 t2
where prev_year_quarter is not null



with 
temp_01 as (
select c.category_name, to_char(date_trunc('month', order_date), 'yyyymm') as month_date
	, sum(oi.amount) as sum_amount
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
	join nw.products p on oi.product_id = p.product_id 
	join nw.categories c on p.category_id = c.category_id 
where o.order_date between to_date('1996-07-01', 'yyyy-mm-dd') and to_date('1997-06-30', 'yyyy-mm-dd')
group by c.category_name, to_char(date_trunc('month', order_date), 'yyyymm')
)
select category_name, month_date, sum_amount
	, first_value(sum_amount) over (partition by category_name order by month_date) as base_amount
	, round(100.0*sum_amount/first_value(sum_amount) over (partition by category_name order by month_date), 2) as base_ratio
from temp_01


with 
temp_01 as (
select to_char(o.order_date, 'yyyymm') as month
	, sum(oi.amount) as monthly_sales
from nw.orders o 
	join nw.order_items oi on o.order_id = oi.order_id 
group by to_char(o.order_date, 'yyyymm')
),
temp_02 as (
select month, substring(month, 1, 4) as year, monthly_sales
	, sum(monthly_sales) over (partition by substring(month, 1, 4) order by month) as cumulative_sales
	, sum(monthly_sales) over (order by month rows between 11 preceding and current row) as moving_annual_sales
from temp_01
)
select *
from temp_02
where year = '1997'




























