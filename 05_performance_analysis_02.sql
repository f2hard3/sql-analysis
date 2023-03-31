with
temp_00 as (
select gu.user_id
	, date_trunc('day', gu.create_time)::date as user_created_date
  	, date_trunc('day', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= (:current_date - interval '8 days') and create_time < :current_date
 group by gu.user_id , date_trunc('day', gu.create_time)::date, date_trunc('day', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , count(distinct t0.user_id) as created_cnt
 	, sum(case when t0.visit_date = (t0.user_created_date + interval '1 days') then 1 else null end) as d1_cnt
	, sum(case when t0.visit_date = (t0.user_created_date + interval '2 days') then 1 else null end) as d2_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '3 days') then 1 else null end) as d3_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '4 days') then 1 else null end) as d4_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '5 days') then 1 else null end) as d5_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '6 days') then 1 else null end) as d6_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '7 days') then 1 else null end) as d7_cnt
from temp_00 t0
group by t0.user_created_date
)
select t1.user_created_date, t1.created_cnt
	, round(100.0 * d1_cnt / t1.created_cnt, 2) as d1_ratio
  	, round(100.0 * d2_cnt / t1.created_cnt, 2) as d2_ratio 
  	, round(100.0 * d3_cnt / t1.created_cnt, 2) as d3_ratio
  	, round(100.0 * d4_cnt / t1.created_cnt, 2) as d4_ratio
  	, round(100.0 * d5_cnt / t1.created_cnt, 2) as d5_ratio
  	, round(100.0 * d6_cnt / t1.created_cnt, 2) as d6_ratio
  	, round(100.0 * d7_cnt / t1.created_cnt, 2) as d7_ratio
from temp_01 t1
order by t1.user_created_date


with
temp_00 as (
select gu.user_id
	, date_trunc('day', gu.create_time)::date as user_created_date
  	, date_trunc('day', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= (:current_date - interval '8 days') and create_time < :current_date
 group by gu.user_id , date_trunc('day', gu.create_time)::date, date_trunc('day', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , count(distinct t0.user_id) as created_cnt
 	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '1 day') then t0.user_id end) as d1_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '2 day') then t0.user_id end) as d2_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '3 day') then t0.user_id end) as d3_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '4 day') then t0.user_id end) as d4_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '5 day') then t0.user_id end) as d5_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '6 day') then t0.user_id end) as d6_cnt
	, count(distinct case when t0.visit_date = (t0.user_created_date + interval '7 day') then t0.user_id end) as d7_cnt
from temp_00 t0
group by t0.user_created_date
)
select t1.user_created_date, t1.created_cnt
	, round(100.0 * d1_cnt / t1.created_cnt, 2) as d1_ratio
  	, round(100.0 * d2_cnt / t1.created_cnt, 2) as d2_ratio 
  	, round(100.0 * d3_cnt / t1.created_cnt, 2) as d3_ratio
  	, round(100.0 * d4_cnt / t1.created_cnt, 2) as d4_ratio
  	, round(100.0 * d5_cnt / t1.created_cnt, 2) as d5_ratio
  	, round(100.0 * d6_cnt / t1.created_cnt, 2) as d6_ratio
  	, round(100.0 * d7_cnt / t1.created_cnt, 2) as d7_ratio
from temp_01 t1
order by t1.user_created_date


with
temp_00 as (
select gu.user_id
	, date_trunc('week', gu.create_time)::date as user_created_date
  	, date_trunc('week', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date
 ('20161101', 'yyyymmdd')
 group by gu.user_id , date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , count(distinct t0.user_id) as created_cnt
 	, sum(case when t0.visit_date = (t0.user_created_date + interval '1 week') then 1 else null end) as w1_cnt
	, sum(case when t0.visit_date = (t0.user_created_date + interval '2 weeks') then 1 else null end) as w2_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '3 weeks') then 1 else null end) as w3_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '4 weeks') then 1 else null end) as w4_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '5 weeks') then 1 else null end) as w5_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '6 weeks') then 1 else null end) as w6_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '7 weeks') then 1 else null end) as w7_cnt
from temp_00 t0
group by t0.user_created_date
)
select t1.user_created_date, t1.created_cnt
	, round(100.0 * w1_cnt / t1.created_cnt, 2) as w1_ratio
  	, round(100.0 * w2_cnt / t1.created_cnt, 2) as w2_ratio 
  	, round(100.0 * w3_cnt / t1.created_cnt, 2) as w3_ratio
  	, round(100.0 * w4_cnt / t1.created_cnt, 2) as w4_ratio
  	, round(100.0 * w5_cnt / t1.created_cnt, 2) as w5_ratio
  	, round(100.0 * w6_cnt / t1.created_cnt, 2) as w6_ratio
  	, round(100.0 * w7_cnt / t1.created_cnt, 2) as w7_ratio
from temp_01 t1
order by t1.user_created_date



with
temp_00 as (
select gu.user_id
	, date_trunc('week', gu.create_time)::date as user_created_date
  	, date_trunc('week', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date('20161101', 'yyyymmdd')
 	and gs.channel_grouping = 'Social'
 group by gu.user_id , date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , count(distinct t0.user_id) as created_cnt
 	, sum(case when t0.visit_date = (t0.user_created_date + interval '1 week') then 1 else null end) as w1_cnt
	, sum(case when t0.visit_date = (t0.user_created_date + interval '2 weeks') then 1 else null end) as w2_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '3 weeks') then 1 else null end) as w3_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '4 weeks') then 1 else null end) as w4_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '5 weeks') then 1 else null end) as w5_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '6 weeks') then 1 else null end) as w6_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '7 weeks') then 1 else null end) as w7_cnt
from temp_00 t0
group by t0.user_created_date
)
select t1.user_created_date, t1.created_cnt
	, round(100.0 * w1_cnt / t1.created_cnt, 2) as w1_ratio
  	, round(100.0 * w2_cnt / t1.created_cnt, 2) as w2_ratio 
  	, round(100.0 * w3_cnt / t1.created_cnt, 2) as w3_ratio
  	, round(100.0 * w4_cnt / t1.created_cnt, 2) as w4_ratio
  	, round(100.0 * w5_cnt / t1.created_cnt, 2) as w5_ratio
  	, round(100.0 * w6_cnt / t1.created_cnt, 2) as w6_ratio
  	, round(100.0 * w7_cnt / t1.created_cnt, 2) as w7_ratio
from temp_01 t1
order by t1.user_created_date


with
temp_00 as (
select gu.user_id , gs.channel_grouping
	, date_trunc('week', gu.create_time)::date as user_created_date
  	, date_trunc('week', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= to_date('20160912', 'yyyymmdd') and create_time < to_date('20160919', 'yyyymmdd')
 group by gu.user_id , gs.channel_grouping , date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , t0.channel_grouping , count(distinct t0.user_id) as created_cnt
 	, sum(case when t0.visit_date = (t0.user_created_date + interval '1 week') then 1 else null end) as w1_cnt
	, sum(case when t0.visit_date = (t0.user_created_date + interval '2 weeks') then 1 else null end) as w2_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '3 weeks') then 1 else null end) as w3_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '4 weeks') then 1 else null end) as w4_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '5 weeks') then 1 else null end) as w5_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '6 weeks') then 1 else null end) as w6_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '7 weeks') then 1 else null end) as w7_cnt
from temp_00 t0
group by t0.user_created_date, t0.channel_grouping
)
select t1.user_created_date, t1.channel_grouping, t1.created_cnt
	, round(100.0 * w1_cnt / t1.created_cnt, 2) as w1_ratio
  	, round(100.0 * w2_cnt / t1.created_cnt, 2) as w2_ratio 
  	, round(100.0 * w3_cnt / t1.created_cnt, 2) as w3_ratio
  	, round(100.0 * w4_cnt / t1.created_cnt, 2) as w4_ratio
  	, round(100.0 * w5_cnt / t1.created_cnt, 2) as w5_ratio
  	, round(100.0 * w6_cnt / t1.created_cnt, 2) as w6_ratio
  	, round(100.0 * w7_cnt / t1.created_cnt, 2) as w7_ratio
from temp_01 t1
order by 3 desc


with
temp_00 as (
select gu.user_id
	, date_trunc('day', gu.create_time)::date as user_created_date
  	, date_trunc('day', gs.visit_stime)::date as visit_date
  	, count(*) as cnt
 from ga.ga_users gu
 	left join ga_sess gs on gu.user_id = gs.user_id 
 where gu.create_time >= (:current_date - interval '8 days') and create_time < :current_date
 group by gu.user_id , date_trunc('day', gu.create_time)::date, date_trunc('day', gs.visit_stime)::date
 ),
 temp_01 as (
 select t0.user_created_date , count(distinct t0.user_id) as created_cnt
 	, sum(case when t0.visit_date = (t0.user_created_date + interval '1 days') then 1 else null end) as d1_cnt
	, sum(case when t0.visit_date = (t0.user_created_date + interval '2 days') then 1 else null end) as d2_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '3 days') then 1 else null end) as d3_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '4 days') then 1 else null end) as d4_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '5 days') then 1 else null end) as d5_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '6 days') then 1 else null end) as d6_cnt
  	, sum(case when t0.visit_date = (t0.user_created_date + interval '7 days') then 1 else null end) as d7_cnt
from temp_00 t0
group by t0.user_created_date
)
select 'All User' as user_create_date, sum(t1.created_cnt) as created_cnt
	, round(100.0 * sum(d1_cnt) / sum(t1.created_cnt), 2) as d1_ratio
  	, round(100.0 * sum(d2_cnt) / sum(t1.created_cnt), 2) as d2_ratio
  	, round(100.0 * sum(d3_cnt) / sum(t1.created_cnt), 2) as d3_ratio
  	, round(100.0 * sum(d4_cnt) / sum(t1.created_cnt), 2) as d4_ratio
  	, round(100.0 * sum(d5_cnt) / sum(t1.created_cnt), 2) as d5_ratio
  	, round(100.0 * sum(d6_cnt) / sum(t1.created_cnt), 2) as d6_ratio
  	, round(100.0 * sum(d7_cnt) / sum(t1.created_cnt), 2) as d7_ratio
from temp_01 t1
union all
select to_char(user_created_date, 'yyyy-mm-dd') as user_create_date, created_cnt
	, round(100.0 * d1_cnt / t1.created_cnt, 2) as d1_ratio
  	, round(100.0 * d2_cnt / t1.created_cnt, 2) as d2_ratio 
  	, round(100.0 * d3_cnt / t1.created_cnt, 2) as d3_ratio
  	, round(100.0 * d4_cnt / t1.created_cnt, 2) as d4_ratio
  	, round(100.0 * d5_cnt / t1.created_cnt, 2) as d5_ratio
  	, round(100.0 * d6_cnt / t1.created_cnt, 2) as d6_ratio
  	, round(100.0 * d7_cnt / t1.created_cnt, 2) as d7_ratio
from temp_01 t1
order by 1


select gsh.action_type , count(*) action_cnt
	, sum(case when hit_type='PAGE' then 1 else 0 end) as page_action_cnt
	, sum(case when hit_type='EVENT' then 1 else 0 end) as event_action_cnt
from ga.ga_sess_hits gsh 
group by gsh.action_type 


with 
temp_00 as (
select count(distinct gsh.sess_id) as purchase_sess_cnt
from ga.ga_sess_hits gsh 
where action_type = '6'
),
temp_01 as (
select count(distinct gsh2.sess_id) as sess_cnt
from ga.ga_sess_hits gsh2 
)
select *
	, round(100.0 * t0.purchase_sess_cnt / t1.sess_cnt, 2) as sales_cv_rate
from temp_00 t0
	cross join temp_01 t1

with 
temp_00 as (
select count(distinct gsh.sess_id) as purchase_sess_cnt
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where action_type = '6'
	and gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
),
temp_01 as (
select count(distinct gsh2.sess_id) as sess_cnt
from ga.ga_sess_hits gsh2 
	join ga.ga_sess gs on gsh2.sess_id = gs.sess_id
where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
)
select *
	, round(100.0 * t0.purchase_sess_cnt / t1.sess_cnt, 2) as sales_cv_rate
from temp_00 t0
	cross join temp_01 t1



select count(distinct gsh.sess_id) as sess_cnt
	, count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt
	, round(100.0 * count(distinct case when gsh.action_type = '6' then gsh.sess_id end) / count(distinct gsh.sess_id), 2) as sales_cv_rate
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date



select date_trunc('day', gs.visit_stime)::date as day_date
	, count(distinct gsh.sess_id) as sess_cnt_by_day
	, count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt_by_day
	, round(100.0 * count(distinct case when gsh.action_type = '6' then gsh.sess_id end) / count(distinct gsh.sess_id), 2) as sales_cv_rate_by_day
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
group by date_trunc('day', gs.visit_stime)::date


select date_trunc('month', gs.visit_stime)::date as month_date
	, count(distinct gsh.sess_id) as sess_cnt_by_month
	, count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt_by_month
	, round(100.0 * count(distinct case when gsh.action_type = '6' then gsh.sess_id end) / count(distinct gsh.sess_id), 2) as sales_cv_rate_by_month
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
group by date_trunc('month', gs.visit_stime)::date


with 
temp_00 as (
select date_trunc('day', gs.visit_stime)::date as day_date
	, count(distinct gsh.sess_id) as sess_cnt_by_day
	, count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt_by_day
	, round(100.0 * count(distinct case when gsh.action_type = '6' then gsh.sess_id end) / count(distinct gsh.sess_id), 2) as sales_cv_rate_by_day
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
group by date_trunc('day', gs.visit_stime)::date
),
temp_01 as (
select date_trunc('day', o.order_time)::date as day_date, 
	sum(oi.prod_revenue) as sum_revenue
from ga.orders o 
	join ga.order_items oi on o.order_id = oi.order_id 
where o.order_time >= (:current_date - interval '7 days') and o.order_time  < :current_date
group by date_trunc('day', o.order_time)::date
)
select *
	, round((t1.sum_revenue / t0.purchase_sess_cnt_by_day)::numeric, 2) as revenue_per_purchase_sess_cnt_by_day
from temp_00 t0
	left join temp_01 t1 on t0.day_date = t1.day_date


with 
temp_00 as (
select gs.channel_grouping , date_trunc('month', gs.visit_stime)::date as visit_month
	, count(distinct gs.sess_id) as sess_cnt
	, count(distinct case when gsh.action_type = '6' then gs.sess_id end) as purchase_sess_cnt
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
group by gs.channel_grouping , date_trunc('month', gs.visit_stime)::date
),
temp_01 as (
select gs2.channel_grouping , date_trunc('month', o.order_time)::date as order_month
	, sum(oi.prod_revenue) as sum_revenue
from ga.ga_sess gs2 
	join ga.orders o on gs2.sess_id = o.sess_id 
	join ga.order_items oi on o.order_id = oi.order_id 
group by gs2.channel_grouping , date_trunc('month', o.order_time)::date
)
select t0.channel_grouping, t0.visit_month, t0.sess_cnt, t0.purchase_sess_cnt
	, round(100.0 * t0.purchase_sess_cnt / t0.sess_cnt, 2) as conversion_rate
	, t1.order_month, round(t1.sum_revenue::numeric, 2) as sum_revenue
	, round(t1.sum_revenue::numeric / purchase_sess_cnt, 2) as revenue_per_purchase_sess
from temp_00 t0
	left join temp_01 t1 on t0.channel_grouping = t1.channel_grouping 
			and t0.visit_month = t1.order_month

create table ga.temp_funnel_base
as
select *
from (
	select gsh.* , gs.visit_stime , gs.channel_grouping 
		, row_number() over(partition by gsh.sess_id, gsh.action_type order by gsh.hit_seq) as action_seq
	from ga.ga_sess_hits gsh 
		join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
	where gs.visit_stime >= (to_date('2016-10-31', 'yyyy-mm-dd') - interval '7 days') and gs.visit_stime < to_date('2016-10-31', 'yyyy-mm-dd') 
) t where t.action_seq = 1

select * from ga.temp_funnel_base tfb 

with
temp_00 as (
select tfb.sess_id , tfb.hit_type , tfb.action_type 
from ga.temp_funnel_base tfb 
where tfb.action_type = '0'
),
temp_01 as (
select tfb0.sess_id as home_sess_id
	, tfb1.sess_id as prod_list_sess_id
	, tfb2.sess_id as prod_detail_sess_id
	, tfb3.sess_id as cart_sess_id
	, tfb5.sess_id as checkout_sess_id
	, tfb6.sess_id as purchase_sess_id
from temp_00 tfb0
	left join ga.temp_funnel_base tfb1 on (tfb0.sess_id = tfb1.sess_id and tfb1.action_type = '1')
	left join ga.temp_funnel_base tfb2 on (tfb1.sess_id = tfb2.sess_id and tfb2.action_type = '2')
	left join ga.temp_funnel_base tfb3 on (tfb2.sess_id = tfb3.sess_id and tfb3.action_type = '3')
	left join ga.temp_funnel_base tfb5 on (tfb3.sess_id = tfb5.sess_id and tfb5.action_type = '5')
	left join ga.temp_funnel_base tfb6 on (tfb5.sess_id = tfb6.sess_id and tfb6.action_type = '6')
)
select count(home_sess_id) home_sess_cnt
	, count(prod_list_sess_id) prod_list_sess_cnt
	, count(prod_detail_sess_id) prod_detail_sess_cnt
	, count(cart_sess_id) cart_sess_cnt
	, count(checkout_sess_id) checkout_sess_cnt
	, count(purchase_sess_id) purchase_sess_cnt
from temp_01

select count(distinct case when tfb.action_type = '0' then tfb.sess_id end) as home_sess_cnt
	, count(distinct case when tfb.action_type = '1' then tfb.sess_id end) as prod_list_sess_cnt
	, count(distinct case when tfb.action_type = '2' then tfb.sess_id end) as prod_detail_sess_cnt
	, count(distinct case when tfb.action_type = '3' then tfb.sess_id end) as cart_sess_cnt
	, count(distinct case when tfb.action_type = '5' then tfb.sess_id end) as checkout_sess_cnt
	, count(distinct case when tfb.action_type = '6' then tfb.sess_id end) as purchase_sess_cnt
from temp_funnel_base tfb 

with
temp_00 as (
select tfb.sess_id , tfb.hit_type , tfb.action_type , tfb.channel_grouping 
from ga.temp_funnel_base tfb 
where tfb.action_type = '0'
),
temp_01 as (
select tfb0.sess_id as home_sess_id , tfb0.channel_grouping as home_channel_grouping
	, tfb1.sess_id as prod_list_sess_id , tfb1.channel_grouping as prod_list_channel_grouping
	, tfb2.sess_id as prod_detail_sess_id , tfb2.channel_grouping as prod_detail_channel_grouping
	, tfb3.sess_id as cart_sess_id , tfb3.channel_grouping as cart_channel_grouping
	, tfb5.sess_id as checkout_sess_id , tfb5.channel_grouping as checkout_channel_grouping
	, tfb6.sess_id as purchase_sess_id , tfb6.channel_grouping as purchase_channel_grouping
from temp_00 tfb0
	left join ga.temp_funnel_base tfb1 on (tfb0.sess_id = tfb1.sess_id and tfb1.action_type = '1')
	left join ga.temp_funnel_base tfb2 on (tfb1.sess_id = tfb2.sess_id and tfb2.action_type = '2')
	left join ga.temp_funnel_base tfb3 on (tfb2.sess_id = tfb3.sess_id and tfb3.action_type = '3')
	left join ga.temp_funnel_base tfb5 on (tfb3.sess_id = tfb5.sess_id and tfb5.action_type = '5')
	left join ga.temp_funnel_base tfb6 on (tfb5.sess_id = tfb6.sess_id and tfb6.action_type = '6')
)
select home_channel_grouping
	, count(home_sess_id) home_sess_cnt
	, count(prod_list_sess_id) prod_list_sess_cnt
	, count(prod_detail_sess_id) prod_detail_sess_cnt
	, count(cart_sess_id) cart_sess_cnt
	, count(checkout_sess_id) checkout_sess_cnt
	, count(purchase_sess_id) purchase_sess_cnt
from temp_01
group by home_channel_grouping




















