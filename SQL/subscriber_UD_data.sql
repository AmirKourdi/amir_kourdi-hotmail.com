
SELECT 

STREAM.subscriber_key as MSISDN,
STREAM_acitiviy_precentence as Streaming_Applications_Precente,
Instant_acitiviy_precentence as Instance_Messaging_Applications_Precente,
WEB_acitiviy_precentence as Web_Applications_Precente,
GAME_acitiviy_precentence as Gaming_Applications_Precente,
OTHER_acitiviy_precentence as Other_Applications_Precente,
RANK () OVER ( 
 ORDER BY stream_volume DESC
 ) Steaming_Applications_Rank,
 RANK () OVER ( 
 ORDER BY web_volume DESC
 ) Web_Applications_Rank,
 RANK () OVER ( 
 ORDER BY game_volume DESC
 ) Game_Applications_Rank,
RANK () OVER ( 
 ORDER BY other_volume DESC
 ) Other_Applications_Rank,
 RANK () OVER ( 
 ORDER BY instant_volume DESC
 ) Instant_Messaging_Applications_Rank



FROM

(SELECT 

	C.subscriber_key,
	application_group_key,
	CAST(SUM(stream_volume) AS DECIMAL(100,2)) as stream_volume ,
	CAST(SUM(activity)/total_activity_time AS DECIMAL(100,2)) as STREAM_acitiviy_precentence,
	total_activity_time


	FROM

		(SELECT 
		
			A.subscriber_key,
			CAST(SUM(total_volume) AS DECIMAL(100,2)) as stream_volume,
			CASE
			   WHEN B.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN B.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN B.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN B.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_group_key,
			
			SUM(activity_time) as activity
			
			FROM
				(SELECT * FROM prod.dwh_faCT_coNV_aGG_daY) A
			
				INNER JOIN
				(SELECT * FROM prod.dwh_dim_apPLICATIONS) B
		
			ON A.application_key = B.application_key
			WHERE application_group_key = 'Streaming Applications'
			AND to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
			GROUP BY subscriber_key, application_group_key) C

		INNER JOIN

		(SELECT subscriber_key, SUM(activity_time) as total_activity_time FROM prod.dwh_faCT_coNV_aGG_daY where to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD') group by subscriber_key ) G

		ON C.subscriber_key = G.subscriber_key 
	GROUP BY 1,2,5
	ORDER BY 1) AS STREAM



INNER JOIN




(SELECT 

	C.subscriber_key,
	application_group_key,
	CAST(SUM(instant_volume) AS DECIMAL(100,2)) as instant_volume ,
	CAST(SUM(activity)/total_activity_time AS DECIMAL(100,2)) as Instant_acitiviy_precentence,
	total_activity_time


	FROM

		(SELECT 
		
			A.subscriber_key,
			CAST(SUM(total_volume) AS DECIMAL(100,2)) as instant_volume,
			CASE
			   WHEN B.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN B.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN B.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN B.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_group_key,
			
			SUM(activity_time) as activity
			
			FROM
				(SELECT * FROM prod.dwh_faCT_coNV_aGG_daY) A
			
				INNER JOIN
				(SELECT * FROM prod.dwh_dim_apPLICATIONS) B
		
			ON A.application_key = B.application_key
			WHERE application_group_key = 'Instant Messaging Applications'
			AND to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
			GROUP BY subscriber_key, application_group_key) C

		INNER JOIN

		(SELECT subscriber_key, SUM(activity_time) as total_activity_time FROM prod.dwh_faCT_coNV_aGG_daY where to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD') group by subscriber_key ) G

		ON C.subscriber_key = G.subscriber_key 
	GROUP BY 1,2,5
	ORDER BY 1) AS Instant



ON STREAM.subscriber_key = Instant.subscriber_key 


INNER JOIN 


	(SELECT 

	C.subscriber_key,
	application_group_key,
	CAST(SUM(web_volume) AS DECIMAL(100,2)) as web_volume ,
	CAST(SUM(activity)/total_activity_time AS DECIMAL(100,2)) as WEB_acitiviy_precentence,
	total_activity_time


	FROM

		(SELECT 
		
			A.subscriber_key,
			CAST(SUM(total_volume) AS DECIMAL(100,2)) as web_volume,
			CASE
			   WHEN B.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN B.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN B.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN B.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_group_key,
			
			SUM(activity_time) as activity
			
			FROM
				(SELECT * FROM prod.dwh_faCT_coNV_aGG_daY) A
			
				INNER JOIN
				(SELECT * FROM prod.dwh_dim_apPLICATIONS) B
		
			ON A.application_key = B.application_key
			WHERE application_group_key = 'Web Applications'
			AND to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
			GROUP BY subscriber_key, application_group_key) C

		INNER JOIN

		(SELECT subscriber_key, SUM(activity_time) as total_activity_time FROM prod.dwh_faCT_coNV_aGG_daY where to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD') group by subscriber_key ) G

		ON C.subscriber_key = G.subscriber_key 
	GROUP BY 1,2,5
	ORDER BY 1) AS WEB





ON STREAM.subscriber_key = WEB.subscriber_key 

INNER JOIN 


	(SELECT 

	C.subscriber_key,
	application_group_key,
	CAST(SUM(game_volume) AS DECIMAL(100,2)) as game_volume,
	CAST(SUM(activity)/total_activity_time AS DECIMAL(100,2)) as GAME_acitiviy_precentence,
	total_activity_time


	FROM

		(SELECT 
		
			A.subscriber_key,
			CAST(SUM(total_volume) AS DECIMAL(100,2)) as game_volume,
			CASE
			   WHEN B.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN B.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN B.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN B.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_group_key,
			
			SUM(activity_time) as activity
			
			FROM
				(SELECT * FROM prod.dwh_faCT_coNV_aGG_daY) A
			
				INNER JOIN
				(SELECT * FROM prod.dwh_dim_apPLICATIONS) B
		
			ON A.application_key = B.application_key
			WHERE application_group_key = 'Games'
			AND to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
			GROUP BY subscriber_key, application_group_key) C

		INNER JOIN

		(SELECT subscriber_key, SUM(activity_time) as total_activity_time FROM prod.dwh_faCT_coNV_aGG_daY where to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD') group by subscriber_key ) G

		ON C.subscriber_key = G.subscriber_key 
	GROUP BY 1,2,5
	ORDER BY 1) AS Games

ON STREAM.subscriber_key = Games.subscriber_key 



INNER JOIN 


	(SELECT 

	C.subscriber_key,
	application_group_key,
	CAST(SUM(other_volume) AS DECIMAL(100,2)) as other_volume,
	CAST(SUM(activity)/total_activity_time AS DECIMAL(100,2)) as OTHER_acitiviy_precentence,
	total_activity_time


	FROM

		(SELECT 
		
			A.subscriber_key,
			CAST(SUM(total_volume) AS DECIMAL(100,2)) as other_volume,
			CASE
			   WHEN B.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
			   WHEN B.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
			   WHEN B.application_group_key IN ('Web Applications') THEN 'Web Applications'
			   WHEN B.application_group_key IN ('Games') THEN 'Games'
			   ELSE 'OTHER'
			END AS application_group_key,
			
			SUM(activity_time) as activity
			
			FROM
				(SELECT * FROM prod.dwh_faCT_coNV_aGG_daY) A
			
				INNER JOIN
				(SELECT * FROM prod.dwh_dim_apPLICATIONS) B
		
			ON A.application_key = B.application_key
			WHERE application_group_key != 'Games' and application_group_key != 'Streaming Applications' and application_group_key != 'Instant Messaging Applications' and application_group_key != 'Web Applications'
			AND to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
			GROUP BY subscriber_key, application_group_key) C

		INNER JOIN

		(SELECT subscriber_key, SUM(activity_time) as total_activity_time FROM prod.dwh_faCT_coNV_aGG_daY where to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD') group by subscriber_key ) G

		ON C.subscriber_key = G.subscriber_key 
	GROUP BY 1,2,5
	ORDER BY 1) AS OTHER

ON STREAM.subscriber_key = OTHER.subscriber_key ;

