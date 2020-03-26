	SELECT  
			A.enforcer_id,
			A.day_date,
			A.device_total_UD_volume_GB,
			B.device_total_volume_GB,
			CAST((A.device_total_UD_volume_GB/B.device_total_volume_GB)*100  AS DECIMAL(100,2)) || ' %' as Percentage_UD_OUT_OF_TOTAL_VOLUME

			
	

	FROM

		(SELECT 
				
			enforcer_id,
			day_date,
			CAST(SUM(total_volume/1024/1024/1024) AS DECIMAL(100,2)) as device_total_UD_volume_GB
	
			
		FROM prod.dwh_faCT_coNV_aGG_daY 
				 
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
		AND subscriber_key = 'UD'
		GROUP by 1,2 ) A
			
			
		INNER JOIN 
		
		(SELECT 
				
			enforcer_id,
			day_date,
			CAST(sum(sum(total_volume/1024/1024/1024)) over (partition by enforcer_id, day_date) AS DECIMAL(100,2)) device_total_volume_GB
			
			
		FROM prod.dwh_faCT_coNV_aGG_daY 
				
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-1,'YYYY-MM-DD')
		GROUP by 1,2) B
		
	ON A.enforcer_id = B.enforcer_id and A.day_date = B.day_date
	
	WHERE device_total_volume_GB > 0
	ORDER BY (A.device_total_UD_volume_GB/B.device_total_volume_GB)*100 DESC;