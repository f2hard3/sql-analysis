with
temp_01 as (
select o.order_id ,o.customer_id ,o.order_date 
	,lag(o.order_date) over (partition by o.customer_id order by o.order_date) as prev_order_date
from nw.orders o 
),
temp_02 as (
select t1.order_id ,t1.customer_id ,t1.order_date, t1.prev_order_date 
	,(t1.order_date - t1.prev_order_date)  as days_since_prev_order
from temp_01 t1
where t1.prev_order_date is not null
)
select * from temp_02

with
temp_01 as (
select o.order_id ,o.customer_id ,o.order_date 
	,lag(o.order_date) over (partition by o.customer_id order by o.order_date) as prev_order_date
from nw.orders o 
),
temp_02 as (
select t1.order_id ,t1.customer_id ,t1.order_date, t1.prev_order_date 
	,(t1.order_date - t1.prev_order_date)  as days_since_prev_order
from temp_01 t1
where t1.prev_order_date is not null
)
select floor(days_since_prev_order / 10.0) * 10 as bin ,count(*) as bin_cnt
from temp_02
group by floor(days_since_prev_order / 10.0) * 10
order by 1

with 
temp_01 as (
select o.customer_id ,date_trunc('month', o.order_date)::date as month_date ,count(*) as order_cnt
from nw.orders o 
group by o.customer_id ,date_trunc('month', o.order_date)::date
)
select * 
from temp_01
order by 2, 1

with 
temp_01 as (
select o.customer_id ,date_trunc('month', o.order_date)::date as month_date ,count(*) as order_cnt
from nw.orders o 
group by o.customer_id ,date_trunc('month', o.order_date)::date
)
select t1.month_date, count(*), avg(t1.order_cnt), max(t1.order_cnt), min(t1.order_cnt)
from temp_01 t1
group by t1.month_date
order by 1

select *
from ga.order_items oi

with
temp_01 as (
select oi.order_id ,oi.product_id as prod_01 ,oi2.product_id as prod_02
from ga.order_items oi 
	join ga.order_items oi2 on oi.order_id = oi2.order_id
where oi.product_id != oi2.product_id 
),
temp_02 as (
select t1.prod_01, t1.prod_02, count(*) as cnt
from temp_01 t1
group by t1.prod_01, t1.prod_02
order by 1, 2, 3
),
temp_03 as (
select t2.prod_01, t2.prod_02, cnt
	,row_number() over (partition by t2.prod_01 order by cnt desc) as rnum
from temp_02 t2
order by 1, 2, 3
)
select * 
from temp_03 t3
where t3.rnum = 1

with 
temp_00 as (
select o.user_id ,o.order_id ,oi.product_id 
from ga.orders o 
	join ga.order_items oi on o.order_id = oi.order_id 
),
temp_01 as (
select t001.user_id ,t001.product_id as prod_01 ,t002.product_id as prod_02
from temp_00 t001
	join temp_00 t002 on t001.user_id = t002.user_id
where t001.product_id != t002.product_id
),
temp_02 as (
select t1.prod_01, t1.prod_02, count(*) cnt
from temp_01 t1
group by t1.prod_01, t1.prod_02
),
temp_03 as (
select t2.prod_01, t2.prod_02, t2.cnt
	,row_number() over(partition by t2.prod_01 order by t2.cnt desc) as rnum
from temp_02 t2
)
select *
from temp_03 t3
where t3.rnum = 1


with
temp_01 as (
select o.user_id , max(date_trunc('day', o.order_time))::date as max_ord_date
	, to_date('20161101', 'yyyymmdd') -  max(date_trunc('day', o.order_time))::date as recency
	, count(distinct o.order_id) as frequency
	, sum(oi.prod_revenue) as monetary
from ga.orders o 
	join ga.order_items oi on o.order_id = oi.order_id 
group by o.user_id 
)
select *
	, ntile(5) over (order by t1.recency asc  rows between unbounded preceding and unbounded following) as recency_rank
	, ntile(5) over (order by t1.frequency desc rows between unbounded preceding and unbounded following) as frequency_rank
	, ntile(5) over (order by t1.monetary desc rows between unbounded preceding and unbounded following) as monetary_rank
from 
temp_01 t1

with
temp_01 as (
select 'A' as grade, 1 as from_recency, 14 as to_recency, 5 as from_frequency, 9999 as to_frequency, 100.0 as from_monetary, 999999.0 as to_monetary
union all
select 'B', 15, 50, 3, 4, 50.0, 99.999
union all
select 'C', 51, 99999, 1, 2, 0.0, 49.999
)
select * from temp_01

with
temp_01 as (
select o.user_id , max(date_trunc('day', o.order_time))::date as max_ord_date
	, to_date('20161101', 'yyyymmdd') -  max(date_trunc('day', o.order_time))::date as recency
	, count(distinct o.order_id) as frequency
	, sum(oi.prod_revenue) as monetary
from ga.orders o 
	join ga.order_items oi on o.order_id = oi.order_id 
group by o.user_id 
),
temp_02 as (
select 'A' as grade, 1 as from_recency, 14 as to_recency, 5 as from_frequency, 9999 as to_frequency, 100.0 as from_monetary, 999999.0 as to_monetary
union all
select 'B', 15, 50, 3, 4, 50.0, 99.999
union all
select 'C', 51, 99999, 1, 2, 0.0, 49.999
),
temp_03 as (
select t1.*
	, t2a.grade as recency_grade, t2b.grade as frequency_grade, t2c.grade as monetary_grade
from temp_01 t1
	left join temp_02 t2a on t1.recency between t2a.from_recency and t2a.to_recency
	left join temp_02 t2b on t1.frequency between t2b.from_frequency and t2b.to_frequency
	left join temp_02 t2c on t1.monetary between t2c.from_monetary and t2c.to_monetary
)
select *
	, case when recency_grade = 'A' and frequency_grade in ('A', 'B') and monetary_grade = 'A' then 'A'
	       when recency_grade = 'B' and frequency_grade = 'A' and monetary_grade = 'A' then 'A'
	       when recency_grade = 'B' and frequency_grade in ('A', 'B', 'C') and monetary_grade = 'B' then 'B'
	       when recency_grade = 'C' and frequency_grade in ('A', 'B') and monetary_grade = 'B' then 'B'
	       when recency_grade = 'C' and frequency_grade = 'C' and monetary_grade = 'A' then 'B'
	       when recency_grade = 'C' and frequency_grade = 'C' and monetary_grade in ('B', 'C') then 'C'
	       when recency_grade in ('B', 'C') and monetary_grade = 'C' then 'C'
	       else 'C' end as total_grade
from temp_03

with
temp_01 as (
select o.user_id , max(date_trunc('day', o.order_time))::date as max_ord_date
	, to_date('20161101', 'yyyymmdd') -  max(date_trunc('day', o.order_time))::date as recency
	, count(distinct o.order_id) as frequency
	, sum(oi.prod_revenue) as monetary
from ga.orders o 
	join ga.order_items oi on o.order_id = oi.order_id 
group by o.user_id 
),
temp_02 as (
select 'A' as grade, 1 as from_recency, 14 as to_recency, 5 as from_frequency, 9999 as to_frequency, 300.0 as from_monetary, 999999.0 as to_monetary
union all
select 'B', 15, 50, 3, 4, 50.0, 299.999
union all
select 'C', 51, 99999, 1, 2, 0.0, 49.999
),
temp_03 as (
select t1.*
	, t2a.grade as recency_grade, t2b.grade as frequency_grade, t2c.grade as monetary_grade
from temp_01 t1
	left join temp_02 t2a on t1.recency between t2a.from_recency and t2a.to_recency
	left join temp_02 t2b on t1.frequency between t2b.from_frequency and t2b.to_frequency
	left join temp_02 t2c on t1.monetary between t2c.from_monetary and t2c.to_monetary
),
temp_04 as (
select *
	, case when recency_grade = 'A' and frequency_grade in ('A', 'B') and monetary_grade = 'A' then 'A'
	       when recency_grade = 'B' and frequency_grade = 'A' and monetary_grade = 'A' then 'A'
	       when recency_grade = 'B' and frequency_grade in ('A', 'B', 'C') and monetary_grade = 'B' then 'B'
	       when recency_grade = 'C' and frequency_grade in ('A', 'B') and monetary_grade = 'B' then 'B'
	       when recency_grade = 'C' and frequency_grade = 'C' and monetary_grade = 'A' then 'B'
	       when recency_grade = 'C' and frequency_grade = 'C' and monetary_grade in ('B', 'C') then 'C'
	       when recency_grade in ('B', 'C') and monetary_grade = 'C' then 'C'
	       else 'C' end as total_grade
from temp_03
)
select total_grade ,'rfm_grade_' || recency_grade || frequency_grade || monetary_grade as rfm_classification
	, count(*) as grade_cnt
from temp_04
group by total_grade ,'rfm_grade_' || recency_grade || frequency_grade || monetary_grade
order by 1




































































































































































