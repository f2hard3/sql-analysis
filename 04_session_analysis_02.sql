with 
temp_00 as (
select gs.sess_id , gs.user_id , gs.visit_stime , gu.create_time 
	, case when gu.create_time >= (:current_date - interval '30 days') and gu.create_time < :current_date then 1 else 0 end as is_new_user
from ga.ga_sess gs 
	join ga.ga_users gu on gs.user_id = gu.user_id 
where gs.visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date 
)
select count(distinct t0.user_id) as user_cnt
	, count(distinct case when t0.is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when t0.is_new_user = 0 then user_id end) as existing_user_cnt
	, count(*) as sess_cnt
from temp_00 t0


select *
from ga.ga_sess gs 

select gs.traffic_medium , gs.traffic_source , count(*)
from ga.ga_sess gs 
group by gs.traffic_medium , gs.traffic_source
order by 3 desc

select gs.channel_grouping , gs.traffic_medium , gs.traffic_source , count(*)
from ga.ga_sess gs 
group by gs.channel_grouping , gs.traffic_medium , gs.traffic_source
order by 4 desc

select gs.channel_grouping , count(distinct gs.user_id)
from ga.ga_sess gs 
group by gs.channel_grouping
order by 2 desc

with 
temp_00 as (
select gs.sess_id , gs.user_id , gs.visit_stime , gu.create_time , gs.channel_grouping 
	, case when gu.create_time >= (:current_date - interval '30 days') and gu.create_time < :current_date then 1 else 0 end as is_new_user
from ga.ga_sess gs 
	join ga.ga_users gu on gs.user_id = gu.user_id 
where gs.visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date 
),
temp_01 as (
select t0.channel_grouping
	, count(distinct case when t0.is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when t0.is_new_user = 0 then user_id end) as existing_user_cnt
	, count(distinct t0.user_id) as channel_user_cnt
	, count(*) as channel_sess_cnt
from temp_00 t0
group by rollup(t0.channel_grouping)
)
select * from temp_01


with 
temp_00 as (
select gs.sess_id , gs.user_id , gs.visit_stime , gu.create_time , gs.channel_grouping 
	, case when gu.create_time >= (:current_date - interval '30 days') and gu.create_time < :current_date then 1 else 0 end as is_new_user
from ga.ga_sess gs 
	join ga.ga_users gu on gs.user_id = gu.user_id 
where gs.visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date 
),
temp_01 as (
select t0.channel_grouping
	, count(distinct case when t0.is_new_user = 1 then user_id end) as new_user_cnt
	, count(distinct case when t0.is_new_user = 0 then user_id end) as existing_user_cnt
	, count(distinct t0.user_id) as channel_user_cnt
	, count(*) as channel_sess_cnt
from temp_00 t0
group by t0.channel_grouping
)
select *
	, round(100.0 * (t1.new_user_cnt /  sum(t1.new_user_cnt) over()), 2) as new_user_cnt_by_channel
	, round(100.0 * (t1.existing_user_cnt /  sum(t1.existing_user_cnt) over()), 2) as exsting_user_cnt_by_channel
from temp_01 t1


with 
temp_00 as (
select gs.sess_id , gs.user_id , gs.channel_grouping , o.order_id , o.order_time , oi.product_id , oi.prod_revenue 
from ga_sess gs 
	left join orders o on gs.sess_id = o.sess_id 
	left join order_items oi on o.order_id = oi.order_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
)
select channel_grouping 
	, sum(t0.prod_revenue) as ch_revenue
	, count(distinct t0.user_id) as ch_user_cnt
	, count(distinct case when t0.order_id is not null then t0.user_id end) as ch_ord_cnt
	, sum(t0.prod_revenue) / count(distinct t0.user_id) as ch_revenue_per_user
	, sum(t0.prod_revenue) / count(distinct case when t0.order_id is not null then t0.user_id end) as ch_ord_revenue_per_user
from temp_00 t0
group by t0.channel_grouping
order by ch_user_cnt desc












































