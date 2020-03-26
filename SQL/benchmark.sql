CREATE Schema PS;

CREATE TABLE PS.Benchmark_Hourly (
   Insert_date_time_stamp       TIMESTAMP,
   day      VARCHAR(64) PRIMARY KEY,
   hour      VARCHAR(64),
   avg_volume_in_gb        INTEGER,
   avg_volume_out_gb         INTEGER,
   avg_volume_total_volume_gb       INTEGER,
   rtt_internal_avg_msec        INTEGER,
   rtt_external_avg_msec        INTEGER,
   retransmission_in        INTEGER,
   retransmission_out        INTEGER,
   UNIQUE (Day,Hour) ENABLED
);


DROP TABLE PS.Benchmark_Hourly;

DROP SCHEMA PS;

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

GROUP BY 1,2
ORDER BY 1,2;





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

where subscriber_reserved_1 = 'CUSTOMER' AND CONV.period_hour_key > (NOW() - INTERVAL '24 hours') 

GROUP BY 1,2
ORDER BY 1,2;













INSERT INTO PS.Benchmark_Hourly
(
SELECT
sysdate()as Insert_date_time_stamp,
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

GROUP BY 1,2,3
ORDER BY 1,2,3
);














SELECT 
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

GROUP BY 1,2
ORDER BY 1,2) CalTable

inner join

PS.Benchmark_Hourly BenchmarkTable

ON BenchmarkTable.day = CalTable.day and BenchmarkTable.hour = CalTable.hour

;





SELECT 
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

GROUP BY 1,2
ORDER BY 1,2) CalTable

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

where subscriber_reserved_1 = 'CUSTOMER') BenchmarkTable

ON BenchmarkTable.day = CalTable.day and BenchmarkTable.hour = CalTable.hour

GROUP BY 1,2;

