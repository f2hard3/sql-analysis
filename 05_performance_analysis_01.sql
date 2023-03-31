select * from ga.ga_sess gs 

select * from ga.ga_sess_hits gsh 

select * from ga.ga_sess_hits gsh where sess_id = 'S0000505' order by hit_seq

select gsh.hit_type , count(*) from ga.ga_sess_hits gsh group by gsh.hit_type 

select gsh.action_type , count(*) from ga.ga_sess_hits gsh group by gsh.action_type 

select gsh.action_type , count(*) as action_cnt
	, sum(case when gsh.hit_type = 'PAGE'then 1 else 0 end) as page_action_cnt
	, sum(case when gsh.hit_type = 'EVENT'then 1 else 0 end) as event_action_cnt
from ga.ga_sess_hits gsh 
group by gsh.action_type  


select gsh.page_path , count(*) as hits_by_page 
from ga_sess_hits gsh
group by gsh.page_path 
order by hits_by_page desc
fetch first 5 rows only

with 
temp_00 as (
select gsh.sess_id , count(*) as hits_by_sess
from ga.ga_sess_hits gsh
group by gsh.sess_id 
)
select count(*), max(t0.hits_by_sess) , avg(t0.hits_by_sess)
	, percentile_disc(0.25) within group(order by t0.hits_by_sess) as percentile_25
	, percentile_disc(0.50) within group(order by t0.hits_by_sess) as percentile_50
	, percentile_disc(0.75) within group(order by t0.hits_by_sess) as percentile_75
	, percentile_disc(1) within group(order by t0.hits_by_sess) as percentile_100 
from temp_00 t0

with
temp_00 as (
select date_trunc('day', gs.visit_stime)::date , count(*) as page_cnt
from ga.ga_sess gs 
	join ga.ga_sess_hits gsh on gs.sess_id = gsh .sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
	and gsh.hit_type = 'PAGE'
group by date_trunc('day', gs.visit_stime)::date 
)
select *,
	avg(t0.page_cnt) over() as avg_page_cnt
from temp_00 t0

select date_trunc('day', gs.visit_stime)::date , count(*) as page_cnt
	, avg(count(*)) over()
from ga.ga_sess gs 
	join ga.ga_sess_hits gsh on gs.sess_id = gsh .sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
	and gsh.hit_type = 'PAGE'
group by date_trunc('day', gs.visit_stime)::date 

with
temp_00 as (
select date_trunc('day', gs.visit_stime)::date , count(*) as page_cnt
from ga.ga_sess gs 
	join ga.ga_sess_hits gsh on gs.sess_id = gsh .sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
	and gsh.hit_type = 'PAGE'
group by date_trunc('day', gs.visit_stime)::date 
),
temp_01 as (
select avg(t0.page_cnt)
from temp_00 t0
)
select * 
from temp_00 t0
	cross join temp_01 t1

with
temp_00 as (
select gsh.page_path , count(*) as hits_by_page 
from ga_sess_hits gsh 
where gsh.hit_type = 'PAGE'
group by gsh.page_path
),
temp_01 as (
select sq.page_path , count(*) as unique_page_cnt
from (
	 select gsh2.sess_id, gsh2.page_path 
		, row_number() over(partition by gsh2.sess_id, gsh2.page_path order by page_path) as rnum
	 from ga.ga_sess_hits gsh2 
	 where hit_type = 'PAGE'
	 ) sq
	 where sq.rnum = 1
	 group by sq.page_path
)
select t0.page_path, t0.hits_by_page, t1.unique_page_cnt
from temp_00 t0
	join temp_01 t1 on t0.page_path = t1.page_path
order by 2 desc


with
temp_00 as (
select gsh.page_path , count(*) as hits_by_page 
from ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gsh.hit_type = 'PAGE'
	and gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
group by gsh.page_path
),
temp_01 as (
select sq.page_path , count(*) as unique_page_cnt
from (
	select distinct gsh2.sess_id, gsh2.page_path 
	from ga.ga_sess_hits gsh2 
		join ga.ga_sess gs on gsh2.sess_id = gs.sess_id 
	where gsh2.hit_type = 'PAGE'
		and gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
) sq group by sq.page_path
)
select t0.page_path, t0.hits_by_page, t1.unique_page_cnt
from temp_00 t0
	join temp_01 t1 on t0.page_path = t1.page_path
order by 2 desc

with 
temp_00 as (
select gsh.sess_id , gsh.page_path , gsh.hit_seq , gsh.hit_time 
	, lead(gsh.hit_time) over(partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
from ga.ga_sess_hits gsh 
where hit_type = 'PAGE'
)
select t0.page_path, count(*) as page_cnt
	, round(avg(t0.next_hit_time - t0.hit_time) / 1000, 2) as avg_elapsed_sec
from temp_00 t0
group by t0.page_path 
order by 2 desc


with
temp_00 as (
select gsh.page_path , count(*) as page_cnt 
from ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gsh.hit_type = 'PAGE'
	and gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
group by gsh.page_path
),
temp_01 as (
select sq.page_path , count(*) as unique_page_cnt
from (
		select distinct gsh2.sess_id, gsh2.page_path 
		from ga.ga_sess_hits gsh2 
			join ga.ga_sess gs on gsh2.sess_id = gs.sess_id 
		where gsh2.hit_type = 'PAGE'
			and gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
	 ) sq group by sq.page_path
), 
temp_02 as (
select gsh.sess_id , gsh.page_path , gsh.hit_seq , gsh.hit_time 
	, lead(gsh.hit_time) over(partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id
where gsh.hit_type = 'PAGE'
	and gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
),
temp_03 as (
select t2.page_path, count(*) as page_cnt
	, round(avg(t2.next_hit_time - t2.hit_time) / 1000.0, 2) as avg_elapsed_sec
from temp_02 t2
group by t2.page_path
)
select a.page_path, a.page_cnt, b.unique_page_cnt, c.avg_elapsed_sec
from temp_00 a
	left join temp_01 b on a.page_path = b.page_path
	left join temp_03 c on a.page_path = c.page_path
order by 2 desc

with
temp_00 as (
select gsh.sess_id , gsh.page_path , gsh.hit_time 
	, lead(gsh.hit_time) over(partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
	, row_number() over(partition by gsh.sess_id , gsh.page_path order by gsh.hit_seq) as rnum
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
	and gsh.hit_type = 'PAGE' 
)
select t0.page_path, count(*) as page_cnt
	, count(case when t0.rnum = 1 then 1 else null end) as unique_page_cnt
	, round(avg(t0.next_hit_time - t0.hit_time) / 1000.0, 2) as avg_elapsed_time
from temp_00 t0
group by t0.page_path
order by 2 desc


with 
temp_00 as (
select gsh.sess_id , gsh.hit_seq , gsh.hit_type , gsh.page_path , is_exit , exit_screen_name
	, first_value(gsh.page_path) over(partition by gsh.sess_id  order by gsh.hit_seq rows between unbounded preceding and current row) as landing_page
	, last_value(gsh.page_path) over(partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and unbounded following) as exit_page
	, case when row_number() over(partition by gsh.sess_id, gsh.hit_type order by gsh.hit_seq desc) = 1 and gsh.hit_type = 'PAGE' then 'True' else '' end as is_page_exit
from ga.ga_sess_hits gsh  
)
select *
from temp_00 t0
--where t0.is_exit != t0.is_page_exit
--where 'googlemerchandisestore.com' || exit_page != regexp_replace(exit_screen_name, 'shop.|www.', '') 



select gsh.sess_id , count(*) , max(gsh.hit_type)
from ga.ga_sess_hits gsh 
group by gsh.sess_id  having count(*) = 1 and max(gsh.hit_Type) = 'EVENT'

with 
temp_00 as (
select gsh.sess_id , count(*) as page_cnt_per_sess
from ga.ga_sess_hits gsh 
group by sess_id 
)
select sum(case when t0.page_cnt_per_sess = 1 then 1 else 0 end) as bounce_cnt
	, count(*) as total_sess_cnt
	, round(100.0 * sum(case when t0.page_cnt_per_sess = 1 then 1 else 0 end) / count(*), 2) as bounce_rate
from temp_00 t0

with
temp_00 as (
select gsh.sess_id , count(*) as hits_by_sess
from ga.ga_sess_hits gsh 
group by gsh.sess_id 
)
select max(t0.hits_by_sess), min(t0.hits_by_sess)
	, percentile_disc(0.25) within group(order by t0.hits_by_sess) as percentile_25
	, percentile_disc(0.5) within group(order by t0.hits_by_sess) as percentile_50
	, percentile_disc(0.75) within group(order by t0.hits_by_sess) as percentile_75
	, percentile_disc(1) within group(order by t0.hits_by_sess) as percentile_100
from temp_00 t0


with
temp_00 as (
select gsh.page_path , gsh.sess_id , gsh.hit_seq , gsh.hit_type , gsh.action_type 
	, count(*) over(partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
	, first_value(gsh.page_path) over(partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date 
and gsh.hit_type = 'PAGE'
),
temp_01 as (
select t0.page_path, count(*) as page_cnt
	, sum(case when t0.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per_page
	, count(distinct case when t0.first_page_path = t0.page_path then sess_id else null end) as sess_cnt_per_page_01
	, count(distinct t0.sess_id) as sess_cnt_per_page_02
from temp_00 t0
group by page_path
)
select *
	, coalesce(round(100.0 * t1.bounce_cnt_per_page / (case when t1.sess_cnt_per_page_01 = 0 then null else t1.sess_cnt_per_page_01 end), 2), 0) as bound_pct_01
	, round(100.0 * t1.bounce_cnt_per_page / t1.sess_cnt_per_page_02, 2) as bounce_pct_02
from temp_01 t1
order by t1.page_cnt desc

with 
temp_00 as (
select gsh.sess_id , gsh.page_path , gsh.hit_seq , gsh.hit_time 
	, lead(hit_time) over(partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
	, row_number() over(partition by gsh.sess_id, gsh.page_path order by gsh.hit_seq) as rnum
	, count(*) over(partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
	, first_value(gsh.page_path) over(partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
),
temp_01 as (
select page_path, count(*) as page_cnt
	, count(case when rnum = 1 then 1 else null end) as unique_page_cnt
	, round(avg(t0.next_hit_time - t0.hit_time) / 1000.0, 2) as avg_elapsed_sec
	, sum(case when t0.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per_page
	, count(distinct case when t0.first_page_path = t0.page_path then sess_id else null end) as sess_cnt_per_page
from temp_00 t0
group by t0.page_path
)
select t1.page_path, t1.page_cnt, t1.unique_page_cnt, t1.avg_elapsed_sec
	, coalesce(round(100.0 * t1.bounce_cnt_per_page / (case when t1.sess_cnt_per_page = 0 then null else t1.sess_cnt_per_page end), 2), 0) as bound_pct
from temp_01 t1
order by t1.page_cnt desc


with 
temp_00 as (
select gsh.page_path , gsh.sess_id , gs.channel_grouping , gsh.hit_seq , gsh.hit_type , gsh.action_type 
	, count(*) over(partition by gsh.sess_id rows between unbounded preceding and unbounded following) as cnt_per_sess
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
)
select t0.channel_grouping, count(*) as page_cnt_per_channel
	, sum(case when t0.cnt_per_sess = 1 then 1 else 0 end) as bounce_cnt
	, count(distinct t0.sess_id) as sess_cnt_per_channel
	, round(100.0 * sum(case when t0.cnt_per_sess = 1 then 1 else 0 end) / count(distinct t0.sess_id), 2) as bounce_pct
from temp_00 t0
group by t0.channel_grouping
order by page_cnt_per_channel desc 


with 
temp_00 as (
select gsh.page_path , gsh.sess_id , gs.channel_grouping , gsh.hit_seq , gsh.hit_type , gsh.action_type 
	, count(*) over(partition by gsh.sess_id rows between unbounded preceding and unbounded following) as cnt_per_sess
	, first_value(gsh.page_path) over(partition by gsh.sess_id order by hit_seq) as first_page_path
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
),
temp_01 as (
select t0.channel_grouping, t0.page_path, count(*) as page_cnt
	, sum(case when t0.cnt_per_sess = 1 then 1 else 0 end) as bounce_cnt
	, count(distinct case when t0.first_page_path = page_path then t0.sess_id else null end) as sess_cnt_per
from temp_00 t0
group by t0.channel_grouping, t0.page_path
)
select *
	, coalesce(round(100.0 * bounce_cnt / (case when sess_cnt_per = 0 then null else sess_cnt_per end), 2), 0) as bounce_pct
from temp_01
order by page_cnt desc, page_path, channel_grouping

with 
temp_00 as (
select gsh.sess_id , gsh.page_path , gsh.hit_seq , gsh.hit_type , gsh.action_type , gsh.is_exit 
	, case when row_number() over(partition by gsh.sess_id order by gsh.hit_seq desc) = 1 then 1 else 0 end as is_exit_page
from ga.ga_sess_hits gsh 
	join ga.ga_sess gs on gsh.sess_id = gs.sess_id 
where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
)
select t0.page_path, count(*) as page_cnt
	, count(distinct t0.sess_id) as sess_cnt
	, count(distinct case when t0.is_exit_page = 1 then t0.sess_id else null end) as exit_cnt
--	, sum(t0.is_exit_page) as exit_cnt
	, round(100.0 * count(distinct case when t0.is_exit_page = 1 then t0.sess_id else null end) / count(distinct t0.sess_id), 2) as exit_rate
from temp_00 t0
group by t0.page_path
order by page_cnt desc

 





















































































































	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

















