SELECT

MSISDN,


			-- PROFILING BASE RANK(VOLUME) <= 100 AND ACTIVITY TIME >= 30% 
			-- All COMBINATION BETWEEN THOSE CRITIRIAONS
CASE		
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) THEN 'Streamer,Messager,Surffer,Gamer'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Streamer,Messager,Surffer'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) THEN 'Streamer,Messager,Gamer'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Streamer,Gamer,Surffer'
	WHEN ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) AND ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) THEN 'Surffer,Gamer,Messager'	
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) THEN 'Streamer,Messager'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Streamer,Surffer'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) THEN 'Streamer,Gamer'
	WHEN ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Messager,Surffer'
	WHEN ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) AND ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) THEN 'Messager,Gamer'
	WHEN ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) AND ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Gamer,Surffer'
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) THEN 'Streamer,Messager'	
	WHEN ( (subs_rank_Streaming_Applications_grp  BETWEEN  0 AND 100 ) OR (Streaming_Applications_Activity >= 30) ) THEN 'Streamer'
	WHEN ( (subs_rank_Instant_Messaging_Applications_grp  BETWEEN  0 AND 100 ) OR (Instant_Messaging_Applications_Activity >= 30) ) THEN 'Messager'
	WHEN ( (subs_rank_Web_Applications_grp  BETWEEN  0 AND 100 ) OR (Web_Applications_Activity >= 30) ) THEN 'Surffer'
	WHEN ( (subs_rank_Games_tv_grp  BETWEEN  0 AND 100 ) OR (Games_Activity >= 30) ) THEN 'Gamer'
	ELSE 'Regular'
END AS User_Profile,

MAX(Streaming_Applications_Activity) as Streaming_Applications_Activity,
MAX(Instant_Messaging_Applications_Activity) as Instant_Messaging_Applications_Activity,
MAX(Web_Applications_Activity) as Web_Applications_Activity,
MAX(Games_Activity) as Games_Activity,
MAX(OTHER_Activity) as OTHER_Activity,
MAX(subs_rank_Streaming_Applications_grp) as Steaming_Applications_Rank,
MAX(subs_rank_Web_Applications_grp) as Web_Applications_Rank,
MAX(subs_rank_Games_tv_grp) as Game_Applications_Rank,
MAX(subs_rank_Instant_Messaging_Applications_grp) as Instant_Messaging_Applications_Rank,
MAX(subs_rank_OTHER_tv_grp) as Other_Applications_Rank



FROM

(select 

post_ranks.subscriber_key as MSISDN,
CAST(post_ranks.Streaming_Applications_Activity*100 AS DECIMAL(100,2))  as Streaming_Applications_Activity, 
CAST(post_ranks.Instant_Messaging_Applications_Activity*100 AS DECIMAL(100,2)) as Instant_Messaging_Applications_Activity, 
CAST(post_ranks.Web_Applications_Activity*100 AS DECIMAL(100,2)) as Web_Applications_Activity,
CAST(post_ranks.Games_Activity*100 AS DECIMAL(100,2)) as Games_Activity,
CAST(post_ranks.OTHER_Activity*100 AS DECIMAL(100,2)) as OTHER_Activity,


case when post_ranks.Streaming_Applications_tv != 0 then
	dense_rank() over (order by post_ranks.Streaming_Applications_tv desc) 
else -1 end as subs_rank_Streaming_Applications_grp,

case when post_ranks.Instant_Messaging_Applications_tv != 0 then
	dense_rank() over (order by post_ranks.Instant_Messaging_Applications_tv desc) 
else -1 end 
	as subs_rank_Instant_Messaging_Applications_grp,

case when post_ranks.Web_Applications_tv != 0 then	
	dense_rank() over ( order by post_ranks.Web_Applications_tv desc) 
else -1 end 
as subs_rank_Web_Applications_grp,

case when post_ranks.Games_tv != 0 then	
	dense_rank() over ( order by post_ranks.Games_tv desc ) 
else -1 end 
 as subs_rank_Games_tv_grp,

case when post_ranks.OTHER_tv != 0 then	 
	dense_rank() over ( order by post_ranks.OTHER_tv desc) 
else -1 end 
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
			where a.period_day_key between trunc(sysdate)-30 and trunc(sysdate)
			and a.subscriber_key != 'UD'
			GROUP by 1,2
	) pre_rnk
	group by 1
) post_ranks ) sub_profile GROUP BY 1,2;