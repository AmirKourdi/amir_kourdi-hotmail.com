
select 

post_ranks.subscriber_key,
CAST(post_ranks.Streaming_Applications_Activity*100 AS DECIMAL(100,2))  as Streaming_Applications_Activity, 
CAST(post_ranks.Instant_Messaging_Applications_Activity*100 AS DECIMAL(100,2)) as Instant_Messaging_Applications_Activity, 
CAST(post_ranks.Web_Applications_Activity*100 AS DECIMAL(100,2)) as Web_Applications_Activity,
CAST(post_ranks.Games_Activity*100 AS DECIMAL(100,2)) as Games_Activity,
CAST(post_ranks.Games_Activity*100 AS DECIMAL(100,2)) as OTHER_Activity,


case when post_ranks.Streaming_Applications_tv != 0 then
	dense_rank() over (order by post_ranks.Streaming_Applications_tv desc) 
else null end as subs_rank_Streaming_Applications_grp,

case when post_ranks.Instant_Messaging_Applications_tv != 0 then
	dense_rank() over (order by post_ranks.Instant_Messaging_Applications_tv desc) 
else null end 
	as subs_rank_Instant_Messaging_Applications_grp,

case when post_ranks.Web_Applications_tv != 0 then	
	dense_rank() over ( order by post_ranks.Web_Applications_tv desc) 
else null end 
as subs_rank_Web_Applications_grp,

case when post_ranks.Games_tv != 0 then	
	dense_rank() over ( order by post_ranks.Games_tv desc ) 
else null end 
 as subs_rank_Games_tv_grp,

case when post_ranks.OTHER_tv != 0 then	 
	dense_rank() over ( order by post_ranks.OTHER_tv desc) 
else null end 
as subs_rank_OTHER_tv_grp

		
from

(
select 
		pre_rnk.subscriber_key,
		--activity rate
		max(decode(pre_rnk.application_grouping ,'Streaming Applications',pre_rnk.activity_per_app_group / pre_rnk.total_activity_per_subs,0)) 
		as Streaming_Applications_Activity,
		max(decode(pre_rnk.application_grouping ,'Instant Messaging Applications',pre_rnk.activity_per_app_group / pre_rnk.total_activity_per_subs,0)) 
		as Instant_Messaging_Applications_Activity,
		max(decode(pre_rnk.application_grouping ,'Web Applications',pre_rnk.activity_per_app_group / pre_rnk.total_activity_per_subs,0))
		as Web_Applications_Activity,
		max(decode(pre_rnk.application_grouping ,'Games',pre_rnk.activity_per_app_group / pre_rnk.total_activity_per_subs,0))
		as Games_Activity,
		max(decode(pre_rnk.application_grouping ,'OTHER',pre_rnk.activity_per_app_group / pre_rnk.total_activity_per_subs,0)) 
		as OTHER_Activity,
--total vol
		max(decode(pre_rnk.application_grouping ,'Streaming Applications',pre_rnk.total_volume,0)) 
		as Streaming_Applications_tv,
		max(decode(pre_rnk.application_grouping ,'Instant Messaging Applications',pre_rnk.total_volume,0)) 
		as Instant_Messaging_Applications_tv,
		max(decode(pre_rnk.application_grouping ,'Web Applications',pre_rnk.total_volume,0)) 
		as Web_Applications_tv,
		max(decode(pre_rnk.application_grouping ,'Games',pre_rnk.total_volume,0)) 
		as Games_tv,
		max(decode(pre_rnk.application_grouping ,'OTHER',pre_rnk.total_volume,0))
		as OTHER_tv
			
from 

	(
		SELECT 
				
			a.subscriber_key,
			CASE
			   WHEN b.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN b.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN b.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN b.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_grouping,
			SUM(a.total_volume) as total_volume,
			SUM(a.activity_time) as activity_per_app_group,
			sum(sum(a.activity_time)) over (partition by A.subscriber_key) total_activity_per_subs
			
			
			FROM
				prod.dwh_faCT_coNV_aGG_daY a 
				inner join
				prod.dwh_dim_apPLICATIONS b
				
			ON a.application_key = b.application_key
			where a.period_day_key between trunc(sysdate)-1 and trunc(sysdate)
			--and a.subscriber_key = '817010425808'
			GROUP by 1,2
	) pre_rnk
	group by 1
) post_ranks ;