	SELECT  
			B.day_date,
			B.source_type_key,
			CASE WHEN A.device_total_UD_volume_GB IS NULL THEN 0.0000
				ELSE A.device_total_UD_volume_GB
			END AS device_total_UD_volume_GB,
			B.device_total_volume_GB,
			CASE WHEN A.device_total_UD_volume_GB IS NULL THEN 0.0000 || '%'
				ELSE CAST((A.device_total_UD_volume_GB/B.device_total_volume_GB)*100  AS DECIMAL(100,4)) || '%' 
			END as Percentage_UD_OUT_OF_TOTAL_VOLUME
			

	

	FROM

		(SELECT 
				
			day_date,
			source_type_key,
			CAST(SUM(total_volume/1024/1024/1024) AS DECIMAL(100,4)) as device_total_UD_volume_GB
	
			
		FROM prod.dwh_faCT_coNV_aGG_day 
				 
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-2,'YYYY-MM-DD')
		AND subscriber_key = 'UD'
		GROUP by 1,2) A
			
			
		RIGHT JOIN 
		
		(SELECT 
				
			day_date,
			source_type_key,
			CAST(sum(total_volume/1024/1024/1024) AS DECIMAL(100,4)) as device_total_volume_GB
			
			
		FROM prod.dwh_faCT_coNV_aGG_day 
				
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate()-2,'YYYY-MM-DD') 
		GROUP by 1,2) B
		
	ON A.day_date = B.day_date and A.source_type_key = B.source_type_key 
	
	ORDER BY device_total_volume_GB DESC;