	SELECT  
			B.enforcer_id,
			B.day_date,
			B.source_type_key,
			B.HOUR,
			CASE WHEN A.device_total_UD_volume_GB IS NULL THEN 0.00
				ELSE A.device_total_UD_volume_GB
			END AS device_total_UD_volume_GB,
			B.device_total_volume_GB,
			CASE WHEN A.device_total_UD_volume_GB IS NULL THEN 0.00 || '%'
				ELSE CAST((A.device_total_UD_volume_GB/B.device_total_volume_GB)*100  AS DECIMAL(100,2)) || ' %' 
			END as Percentage_UD_OUT_OF_TOTAL_VOLUME
			

	

	FROM

		(SELECT 
				
			enforcer_id,
			day_date,
			source_type_key,
			period_hour_key as HOUR,
			CAST(SUM(total_volume/1024/1024/1024) AS DECIMAL(100,4)) as device_total_UD_volume_GB
	
			
		FROM prod.dwh_faCT_coNV_aGG_hoUR 
				 
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate(),'YYYY-MM-DD')
		AND subscriber_key = 'UD' AND source_type_key != 20
		GROUP by 1,2,3,4 ) A
			
			
		RIGHT JOIN 
		
		(SELECT 
				
			enforcer_id,
			day_date,
			source_type_key,
			period_hour_key as HOUR,
			CAST(sum(sum(total_volume/1024/1024/1024)) over (partition by enforcer_id, period_hour_key) AS DECIMAL(100,4)) device_total_volume_GB
			
			
		FROM prod.dwh_faCT_coNV_aGG_hoUR 
				
		WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate(),'YYYY-MM-DD') AND source_type_key != 20
		GROUP by 1,2,3,4) B
		
	ON A.enforcer_id = B.enforcer_id and A.day_date = B.day_date and A.HOUR = B.HOUR 
	
	ORDER BY device_total_volume_GB DESC;