select * from ga.ga_sess gs 

select gs.browser , count(*)
from ga.ga_sess gs
group by gs.browser 
order by 2 desc

select gs.channel_grouping , count(*)
from ga.ga_sess gs
group by gs.channel_grouping
order by 2 desc

select user_id , count(*)
from ga.ga_sess gs 
group by gs.user_id 
having count(*) > 0
order by 2 desc

select * from ga.ga_users gu 

select * from ga.orders o 

select * from ga.ga_sess_hits gsh 

select * 
from ga.ga_sess_hits gsh 
where sess_id = 'S0000505'
order by hit_seq



with 
temp_01 as (
select to_char(date_trunc('day', gs.visit_stime), 'yyyy-mm-dd') as day_date
	, count(distinct sess_id) as daily_sess_cnt
	, count(distinct gs.user_id) as daily_user_cnt	
from ga.ga_sess gs 
group by to_char(date_trunc('day', gs.visit_stime), 'yyyy-mm-dd') 
)
select *
	, 1.0 * daily_sess_cnt / daily_user_cnt as avg_daily_sess_by_users
from temp_01


select date_trunc('day', gs.visit_stime)::date as day_date
	, count(distinct gs.user_id) as dau
from ga.ga_sess gs 
group by date_trunc('day', gs.visit_stime)::date

select date_trunc('week', gs.visit_stime)::date as week_date
	, count(distinct gs.user_id) as wau
from ga.ga_sess gs 
group by date_trunc('week', gs.visit_stime)::date

select date_trunc('month', gs.visit_stime)::date as month_date
	, count(distinct gs.user_id) as mau
from ga.ga_sess gs 
group by date_trunc('month', gs.visit_stime)::date


select to_date('20161101', 'yyyymmdd') - interval '7 days'


select to_date('20161101', 'yyyymmdd')
	, count(distinct gs.user_id) as wau
from ga.ga_sess gs 
where gs.visit_stime >= (to_date('20161101', 'yyyymmdd') - interval '7 days') and visit_stime < to_date('20161101', 'yyyymmdd')

select :current_date
	, count(distinct gs.user_id) as dau
from ga.ga_sess gs 
where gs.visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date  

select :current_date
	, count(distinct gs.user_id) as wau
from ga.ga_sess gs 
where gs.visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date  

select :current_date
	, count(distinct gs.user_id) as mau
from ga.ga_sess gs 
where gs.visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date  


create table if not exists daily_acqusitions
(
day_date date,
dau integer,
wau integer,
mau integer
)

select *
from ga.daily_acqusitions da 

insert into daily_acqusitions
select :current_date 
	, (select count(distinct gs.user_id) as dau
	   from ga.ga_sess gs 
	   where gs.visit_stime >= (:current_date - interval '1 days') and visit_stime < :current_date
	  )
    , (select count(distinct gs.user_id) as wau
	   from ga.ga_sess gs 
	   where gs.visit_stime >= (:current_date - interval '7 days') and visit_stime < :current_date
	  )
	, (select count(distinct gs.user_id) as mau
	   from ga.ga_sess gs 
	   where gs.visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
	  )


select generate_series('2016-08-02'::date, '2016-11-01'::date, interval '1 day')::date as curr_date

select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date

with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date, count(distinct user_id) dau
from  ga.ga_sess gs 
	cross join temp_00 t0
where visit_stime >= (t0.curr_date - interval '1 day') and visit_stime < t0.curr_date
group by t0.curr_date

with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date, count(distinct user_id) wau
from  ga.ga_sess gs 
	cross join temp_00 t0
where visit_stime >= (t0.curr_date - interval '7 days') and visit_stime < t0.curr_date
group by t0.curr_date

with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date, count(distinct user_id) mau
from  ga.ga_sess gs 
	cross join temp_00 t0
where visit_stime >= (t0.curr_date - interval '30 days') and visit_stime < t0.curr_date
group by t0.curr_date

select count(distinct gs.user_id) as mau
from ga.ga_sess gs 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date

drop table if exists dau

create table if not exists dau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date
	, count(distinct gs.user_id) as dau
from ga.ga_sess gs 
	cross join temp_00 t0
where gs.visit_stime  >= (t0.curr_date - interval '1 day') and gs.visit_stime < t0.curr_date 
group by t0.curr_date

select * from dau

drop table if exists wau

create table if not exists wau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date
	, count(distinct gs.user_id) as wau
from ga.ga_sess gs 
	cross join temp_00 t0
where gs.visit_stime  >= (t0.curr_date - interval '7 day') and gs.visit_stime < t0.curr_date 
group by t0.curr_date

select * from wau

drop table if exists mau

create table if not exists mau
as
with 
temp_00 as (
select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date 
)
select t0.curr_date
	, count(distinct gs.user_id) as mau
from ga.ga_sess gs 
	cross join temp_00 t0
where gs.visit_stime  >= (t0.curr_date - interval '30 day') and gs.visit_stime < t0.curr_date 
group by t0.curr_date

select * from mau
 
drop table if exists daily_acquisitions

create table daily_acquisitions 
as
select d."curr_date" , d.dau , w.wau , m.mau 
from ga.dau d
	join ga.wau w on d."curr_date" = w."curr_date" 
	join ga.mau m on w."curr_date" = m."curr_date" 

select * from ga.daily_acquisitions da 


with
temp_dau as (
select :current_date as curr_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs 
where visit_stime >= (:current_date - interval '1 day') and visit_stime < :current_date
),
temp_mau as (
select :current_date as curr_date, count(distinct gs.user_id) as mau
from ga.ga_sess gs 
where visit_stime >= (:current_date - interval '30 days') and visit_stime < :current_date
)
select td.curr_date, td.dau , tm.mau , round(100.0 * td.dau / tm.mau, 2) as stickiness
from temp_dau td
	join temp_mau tm on td.curr_date = tm.curr_date

select *
	, round(100.0 * da.dau / da.mau, 2) as stickiness
	, round(avg(100.0 * da.dau / da.mau) over (), 2) as avg_stickiness
from ga.daily_acquisitions da
where curr_date between to_date('2016-10-25', 'yyyy-mm-dd') and to_date('2016-10-31', 'yyyy-mm-dd')


select gu.user_id 
	, date_trunc('month', gs.visit_stime)::date as month_date 
	, count(*) as monthly_cnt
from ga.ga_sess gs 
	join ga.ga_users gu on gs.user_id = gu.user_id 
where gu.create_time <= (date_trunc('month', gu.create_time) + interval '1 month' - interval '1 day')::date - 2
group by gu.user_id , date_trunc('month', gs.visit_stime)::date 
order by 1, 2


with 
temp_01 as (
select gu.user_id , date_trunc('month', gs.visit_stime)::date as month_date , count(*)  as monthly_cnt
from ga.ga_sess gs  
	join ga.ga_users gu on gs.user_id = gu.user_id 
where gu.create_time <= (date_trunc('month', gu.create_time) + interval '1 month' - interval '1 day')::date - 2
group by gu.user_id , date_trunc('month', gs.visit_stime)::date
),
temp_02 as (
select t1.month_date
	, case when monthly_cnt = 1 then '0_only_first_session'
		   when monthly_cnt between 2 and 3 then '2_between_3'
		   when monthly_cnt between 4 and 8 then '4_between_8'
		   when monthly_cnt between 9 and 14 then '9_between_14'
		   when monthly_cnt between 15 and 25 then '15_between_25'
		   when monthly_cnt >= 26 then 'over_26' end as classification
	, count(*) as cnt
from temp_01 t1
group by t1.month_date
	, case when monthly_cnt = 1 then '0_only_first_session'
		   when monthly_cnt between 2 and 3 then '2_between_3'
		   when monthly_cnt between 4 and 8 then '4_between_8'
		   when monthly_cnt between 9 and 14 then '9_between_14'
		   when monthly_cnt between 15 and 25 then '15_between_25'
		   when monthly_cnt >= 26 then 'over_26' end
)
select t2.month_date
	, sum(case when t2.classification = '0_only_first_session' then t2.cnt else 0 end) as "0_only_first_session"
	, sum(case when t2.classification = '2_between_3' then t2.cnt else 0 end) as "2_between_3"
	, sum(case when t2.classification = '4_between_8' then t2.cnt else 0 end) as "4_between_8"
	, sum(case when t2.classification = '9_between_14' then t2.cnt else 0 end) as "9_between_14"
	, sum(case when t2.classification = '15_between_25' then t2.cnt else 0 end) as "15_between_25"
	, sum(case when t2.classification = 'over_26' then t2.cnt else 0 end) as "over_26"
from temp_02 t2
group by t2.month_date



select gs.user_id 
	, row_number () over (partition by user_id order by gs.visit_stime) as session_rnum
	, gs.visit_stime 
	, count(*) over (partition by user_id) as session_cnt
from ga_sess gs 
order by 1, 2

with 
temp_00 as (
select gs.user_id 
	, row_number () over (partition by user_id order by gs.visit_stime) as session_rnum
	, gs.visit_stime 
	, count(*) over (partition by user_id) as session_cnt
from ga_sess gs 
),
temp_01 as (
select t0.user_id
	, max(t0.visit_stime) - min(t0.visit_stime) as sess_time_diff
from temp_00 t0
where t0.session_rnum <= 2 and t0.session_cnt > 1
group by t0.user_id
)
select justify_interval(avg(t1.sess_time_diff)) as avg_time
	, max(t1.sess_time_diff) as max_time
	, min(t1.sess_time_diff) as min_time
	, percentile_disc(0.25) within group (order by t1.sess_time_diff)  as percentile_1
	, percentile_disc(0.5) within group (order by t1.sess_time_diff)  as percentile_2
	, percentile_disc(0.75) within group (order by t1.sess_time_diff)  as percentile_3
	, percentile_disc(1.0) within group (order by t1.sess_time_diff)  as percentile_4
from temp_01 t1
where t1.sess_time_diff::interval > interval '0 second'


































