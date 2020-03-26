
SELECT 
CalTable.DATE,
CalTable.day,
CalTable.HOUR,
(CAST((CalTable.avg_volume_in_gb/BenchmarkTable.avg_volume_in_gb) AS DECIMAL(100,2)) - 1.00) as ratio_in_volume,
(CAST((CalTable.avg_volume_out_gb/BenchmarkTable.avg_volume_out_gb) AS DECIMAL(100,2)) - 1.00) as ratio_out_volume,
(CAST((CalTable.avg_volume_total_volume_gb/BenchmarkTable.avg_volume_total_volume_gb) AS DECIMAL(100,2)) - 1.00)as ratio_total_volume,
(CAST((CalTable.rtt_internal_avg_msec/BenchmarkTable.rtt_internal_avg_msec) AS DECIMAL(100,2)) - 1.00) as ratio_rtt_internal,
(CAST((CalTable.rtt_external_avg_msec/BenchmarkTable.rtt_external_avg_msec) AS DECIMAL(100,2)) - 1.00) as ratio_rtt_external,
(CAST((CalTable.retransmission_in/BenchmarkTable.retransmission_in) AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_in,
(CAST((CalTable.retransmission_out/BenchmarkTable.retransmission_out) AS DECIMAL(100,2)) - 1.00) as ratio_retransmission_out

FROM

(SELECT
max((date_trunc('day', CONV.period_min_key))::date) as DATE,
CASE
        WHEN DAYOFWEEK(CONV.period_min_key) = 1 THEN 'Sunday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 2 THEN 'Monday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 3 THEN 'Tuesday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 4 THEN 'Wensday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 5 Then 'Thursday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 6 Then 'Friday'
        WHEN DAYOFWEEK(CONV.period_min_key) = 7 Then 'Saturday'
        ELSE 'N/A'
END AS day,
TO_CHAR(timestamp_trunc(CONV.period_min_key ,'HH24'),'HH:MI') as HOUR,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.period_min_key::date) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.period_min_key::date) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.period_min_key::date) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(SUM(CONV.rtt_internal_avg_msec*CONV.total_volume)/sum(CONV.total_volume) AS DECIMAL(100,2)) AS rtt_internal_avg_msec,
CAST(SUM(CONV.rtt_external_avg_msec*CONV.total_volume)/sum(CONV.total_volume) AS DECIMAL(100,2)) AS rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out
FROM PROD.DWH_FACT_CONV_RAW CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   ON CONV.subscriber_key = DIMSUB.subscriber_key
where subscriber_reserved_1 = 'CUSTOMER' AND timestamp_trunc(CONV.period_min_key, 'HH24') = (timestamp_trunc(NOW(), 'HH24') - INTERVAL '2 HOURS')
GROUP BY 2,3
ORDER BY 2,3) CalTable

inner join

(
SELECT 
CASE
	WHEN DAYOFWEEK(CONV.period_hour_key) = 1 THEN 'Sunday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 2 THEN 'Monday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 3 THEN 'Tuesday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 4 THEN 'Wensday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 5 Then 'Thursday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 6 Then 'Friday'
	WHEN DAYOFWEEK(CONV.period_hour_key) = 7 Then 'Saturday'
	ELSE 'N/A'
END AS day,
TO_CHAR(period_hour_key, 'HH:MI') as HOUR,
CAST(SUM(CONV.volume_in/1024/1024/1024)/COUNT(DISTINCT CONV.period_hour_key) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024)/COUNT(DISTINCT CONV.period_hour_key) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024)/COUNT(DISTINCT CONV.period_hour_key) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_agg_hour CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   
   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER'
GROUP BY 1,2
ORDER BY 1,2) BenchmarkTable

ON BenchmarkTable.day = CalTable.day and BenchmarkTable.hour = CalTable.hour
ORDER BY 1,2;