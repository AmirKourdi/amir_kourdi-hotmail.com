
SELECT 
CalTable.date,
CalTable.day,
(CAST(CalTable.avg_volume_in_gb/BenchmarkTable.avg_volume_in_gb AS DECIMAL(100,2)) - 1.00) as ratio_in_volume,
(CAST(CalTable.avg_volume_out_gb/BenchmarkTable.avg_volume_out_gb AS DECIMAL(100,2)) - 1.00) as ratio_out_volume,
(CAST(CalTable.avg_volume_total_volume_gb/BenchmarkTable.avg_volume_total_volume_gb AS DECIMAL(100,2)) - 1.00) as ratio_total_volume,
(CAST(CalTable.rtt_internal_avg_msec/BenchmarkTable.rtt_internal_avg_msec AS DECIMAL(100,2)) - 1.00) as ratio_rtt_internal,
(CAST(CalTable.rtt_external_avg_msec/BenchmarkTable.rtt_external_avg_msec AS DECIMAL(100,2)) - 1.00) as ratio_rtt_external,
(CAST(CalTable.retransmission_in/BenchmarkTable.retransmission_in AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_in,
(CAST(CalTable.retransmission_out/BenchmarkTable.retransmission_out AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_out

FROM

(SELECT 
CONV.day_date as date,
CASE
	WHEN DAYOFWEEK(CONV.day_date) = 1 THEN 'Sunday'
	WHEN DAYOFWEEK(CONV.day_date) = 2 THEN 'Monday'
	WHEN DAYOFWEEK(CONV.day_date)= 3 THEN 'Tuesday'
	WHEN DAYOFWEEK(CONV.day_date) = 4 THEN 'Wensday'
	WHEN DAYOFWEEK(CONV.day_date) = 5 Then 'Thursday'
	WHEN DAYOFWEEK(CONV.day_date) = 6 Then 'Friday'
	WHEN DAYOFWEEK(CONV.day_date) = 7 Then 'Saturday'
	ELSE 'N/A'
END AS day,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_agg_day CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   
   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER' AND CONV.period_day_key > (NOW() - INTERVAL '30 days') 

GROUP BY 1,2
ORDER BY 1,2) CalTable

inner join

(
SELECT 
CASE
	WHEN DAYOFWEEK(CONV.day_date) = 1 THEN 'Sunday'
	WHEN DAYOFWEEK(CONV.day_date) = 2 THEN 'Monday'
	WHEN DAYOFWEEK(CONV.day_date)= 3 THEN 'Tuesday'
	WHEN DAYOFWEEK(CONV.day_date) = 4 THEN 'Wensday'
	WHEN DAYOFWEEK(CONV.day_date) = 5 Then 'Thursday'
	WHEN DAYOFWEEK(CONV.day_date) = 6 Then 'Friday'
	WHEN DAYOFWEEK(CONV.day_date) = 7 Then 'Saturday'
	ELSE 'N/A'
END AS day,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.day_date) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_agg_day CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   
   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER'
GROUP BY 1
ORDER BY 1) BenchmarkTable

ON BenchmarkTable.day = CalTable.day 
ORDER BY 1,2;