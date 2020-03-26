
SELECT 
CalTable.week_number,
CalTable.day_type,
CalTable.application_grouping,
(CAST(CalTable.avg_volume_in_gb/BenchmarkTable.avg_volume_in_gb AS DECIMAL(100,2)) - 1.00) as ratio_in_volume,
(CAST(CalTable.avg_volume_out_gb/BenchmarkTable.avg_volume_out_gb AS DECIMAL(100,2)) - 1.00) as ratio_out_volume,
(CAST(CalTable.avg_volume_total_volume_gb/BenchmarkTable.avg_volume_total_volume_gb AS DECIMAL(100,2)) - 1.00) as ratio_total_volume,
(CAST(CalTable.rtt_internal_avg_msec/BenchmarkTable.rtt_internal_avg_msec AS DECIMAL(100,2)) - 1.00) as ratio_rtt_internal,
(CAST(CalTable.rtt_external_avg_msec/BenchmarkTable.rtt_external_avg_msec AS DECIMAL(100,2)) - 1.00) as ratio_rtt_external,
(CAST(CalTable.retransmission_in/BenchmarkTable.retransmission_in AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_in,
(CAST(CalTable.retransmission_out/BenchmarkTable.retransmission_out AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_out

FROM

(SELECT 
CONV.week_number as week_number,
CASE
	WHEN DAYOFWEEK(CONV.day_date) = 1 THEN 'Weekend'
	WHEN DAYOFWEEK(CONV.day_date) = 2 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 3 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 4 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 5 Then 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 6 Then 'Weekend'
	WHEN DAYOFWEEK(CONV.day_date) = 7 Then 'Weekend'
	ELSE 'N/A'
END AS day_type,
CASE
   WHEN APPGROUP.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
   WHEN APPGROUP.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
   WHEN APPGROUP.application_group_key IN ('Web Applications') THEN 'Web Applications'
   WHEN APPGROUP.application_group_key IN ('Games') THEN 'Games'
   ELSE 'OTHER'
END AS application_grouping,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_agg_day CONV
				inner join
				prod.dwh_dim_apPLICATIONS APPGROUP
				
				ON  CONV.application_key = APPGROUP.application_key
				
				inner join
				prod.dwh_dim_suBSCRIBERs DIMSUB
				ON CONV.subscriber_key = DIMSUB.subscriber_key
   
where subscriber_reserved_1 = 'CUSTOMER' AND CONV.period_day_key > (NOW() - INTERVAL '30 days') 

GROUP BY 1,2,3
ORDER BY 1,2,3) CalTable

inner join

(
SELECT 
CASE
	WHEN DAYOFWEEK(CONV.day_date) = 1 THEN 'Weekend'
	WHEN DAYOFWEEK(CONV.day_date) = 2 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 3 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 4 THEN 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 5 Then 'Weekday'
	WHEN DAYOFWEEK(CONV.day_date) = 6 Then 'Weekend'
	WHEN DAYOFWEEK(CONV.day_date) = 7 Then 'Weekend'
	ELSE 'N/A'
END AS day_type,
CASE
   WHEN APPGROUP.application_group_key IN ('Streaming Applications') THEN 'Streaming Applications'
   WHEN APPGROUP.application_group_key IN ('Instant Messaging Applications') THEN 'Instant Messaging Applications'
   WHEN APPGROUP.application_group_key IN ('Web Applications') THEN 'Web Applications'
   WHEN APPGROUP.application_group_key IN ('Games') THEN 'Games'
   ELSE 'OTHER'
END AS application_grouping,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_agg_day CONV
				inner join
				prod.dwh_dim_apPLICATIONS APPGROUP
				
				ON  CONV.application_key = APPGROUP.application_key
				
				inner join
				prod.dwh_dim_suBSCRIBERs DIMSUB
				ON CONV.subscriber_key = DIMSUB.subscriber_key
   

where DIMSUB.subscriber_reserved_1 = 'CUSTOMER'
GROUP BY 1,2
ORDER BY 1,2) BenchmarkTable

ON BenchmarkTable.day_type = CalTable.day_type 
AND
BenchmarkTable.application_grouping = CalTable.application_grouping

ORDER BY 1,2,3;