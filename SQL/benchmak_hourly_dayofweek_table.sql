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
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out,
4 as HISTORICAL_COUNTER
FROM prod.dwh_fACT_coNV_agg_hour CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB

   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER'
GROUP BY 1,2
ORDER BY 1,2;
