SELECT 
	 enforcer_id AS SERVICE_GATEWAY,
     max((date_trunc('day', period_min_key))::date) as DATE,
     nvl(timestamp_trunc(DWH_FACT_CONV_RAW.period_min_key ,'HH24'),'2000-01-01 00:00:00'::timestamp) as HOUR,
     CAST(SUM(volume_in/1024/1024/1024) AS DECIMAL(100,2)) as VOLUME_IN_GB,
     CAST(SUM(volume_out/1024/1024/1024) AS DECIMAL(100,2)) as VOLUME_OUT_GB,
	 CAST(SUM(volume_in+volume_out)/1024/1024/1024 AS DECIMAL(100,2)) as TOTAL_VOLUME_GB,
	 CAST(SUM(rtt_internal_avg_msec*total_volume)/sum(total_volume) AS DECIMAL(100,2)) AS Weighted_AVG_RTT_INTERNAL,
	 CAST(SUM(rtt_external_avg_msec*total_volume)/sum(total_volume) AS DECIMAL(100,2)) AS Weighted_AVG_RTT_EXTERNAL,
	 SUM(RTT_in_sum) as RTT_in_sum,
	 SUM(RTT_out_sum) as RTT_out_sum,
	 SUM(RTT_in_events) as RTT_in_events,
	 SUM(RTT_out_events) as RTT_out_events,
	 CAST(SUM(RTT_in_sum)/nullifzero(SUM(RTT_in_events))  AS DECIMAL(100,2)) as REGULAR_AVG_RTT_IN,
	 CAST(SUM(RTT_out_sum)/nullifzero(SUM(RTT_out_events)) AS DECIMAL(100,2)) as REGULAR_AVG_RTT_OUT

     FROM PROD.DWH_FACT_CONV_RAW

	 WHERE RTT_in_events > 0 AND RTT_out_events > 0

	 GROUP BY 1,3
	 ORDER BY 10 DESC;