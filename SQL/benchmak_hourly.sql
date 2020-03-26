
SELECT 
CalTable.date,
CalTable.day as day,
CalTable.hour as hour,
CAST(BenchmarkTable.avg_volume_in_gb/CalTable.avg_volume_in_gb AS DECIMAL(100,2)) as ratio_in_volume,
CAST(BenchmarkTable.avg_volume_out_gb/CalTable.avg_volume_out_gb AS DECIMAL(100,2)) as ratio_out_volume,
CAST(BenchmarkTable.avg_volume_total_volume_gb/CalTable.avg_volume_total_volume_gb AS DECIMAL(100,2))as ratio_total_volume,
CAST(BenchmarkTable.rtt_internal_avg_msec/CalTable.rtt_internal_avg_msec AS DECIMAL(100,2)) as ratio_rtt_internal,
CAST(BenchmarkTable.rtt_external_avg_msec/CalTable.rtt_external_avg_msec AS DECIMAL(100,2)) as ratio_rtt_external,
CAST(BenchmarkTable.retransmission_in/CalTable.retransmission_in AS DECIMAL(100,2)) as ratio_retransmission_in,
CAST(BenchmarkTable.retransmission_out/CalTable.retransmission_out AS DECIMAL(100,2)) as ratio_retransmission_out

FROM

(SELECT 
CONV.day_date as date,
DAYOFWEEK(CONV.day_date) as day,
to_char(CONV.period_hour_key,'HH:MI') as hour,
CAST(SUM(CONV.volume_in/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_aGG_hOUR CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   
   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER' AND CONV.period_hour_key > (NOW() - INTERVAL '24 hours') 

GROUP BY 1,2,3
ORDER BY 1,2,3) CalTable

inner join

(
SELECT 
DAYOFWEEK(CONV.day_date) as day,
to_char(CONV.period_hour_key,'HH:MI') as hour,
CAST(SUM(CONV.volume_in/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_in_gb,
CAST(SUM(CONV.volume_out/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_out_gb,
CAST(SUM(CONV.total_volume/1024/1024/1024) AS DECIMAL(100,2)) as avg_volume_total_volume_gb,
CAST(avg(CONV.rtt_internal_avg_msec) AS DECIMAL(100,2)) as rtt_internal_avg_msec,
CAST(avg(CONV.rtt_external_avg_msec) AS DECIMAL(100,2)) as rtt_external_avg_msec,
CAST(avg(CONV.dropped_packets_in) AS DECIMAL(100,2)) as retransmission_in,
CAST(avg(CONV.dropped_packets_out) AS DECIMAL(100,2)) as retransmission_out

FROM prod.dwh_fACT_coNV_aGG_hOUR CONV
     inner join
     prod.dwh_dim_suBSCRIBERs DIMSUB
   
   ON CONV.subscriber_key = DIMSUB.subscriber_key

where subscriber_reserved_1 = 'CUSTOMER'
GROUP BY 1,2) BenchmarkTable

ON BenchmarkTable.day = CalTable.day and BenchmarkTable.hour = CalTable.hour
ORDER BY 1,2,3;