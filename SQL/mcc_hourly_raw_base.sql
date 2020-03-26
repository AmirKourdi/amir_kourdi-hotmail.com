
SELECT

     SDRtable.sgsn_mcc_mnc,
     COUNT(distinct SDRtable.subscriber_key) as number_of_subscribers,
     SDRtable.date,
     SDRtable.hour,
     CAST(SUM(CONVtable.volume_in/1024/1024) AS DECIMAL(100,2)) as volume_in_MB,
     CAST(SUM(CONVtable.volume_out/1024/1024) AS DECIMAL(100,2)) as volume_out_MB,
     CAST((SUM(CONVtable.volume_in/1024/1024) + SUM(CONVtable.volume_out/1024/1024)) AS DECIMAL(100,2)) as total_volume_MB


FROM
     (SELECT
     DWH_FACT_SDR_RAW.sgsn_mcc_mnc,
     nvl(DWH_FACT_SDR_RAW.subscriber_key,'UD') as subscriber_key ,
     max((date_trunc('day', DWH_FACT_SDR_RAW.period_min_key))::date) as date,
     nvl(timestamp_trunc(DWH_FACT_SDR_RAW.period_min_key ,'HH24'),'2000-01-01 00:00:00'::timestamp) as hour
     FROM PROD.DWH_FACT_SDR_RAW
     group by 1,2,4) SDRtable

     INNER JOIN

     (SELECT subscriber_key,
     nvl(timestamp_trunc(DWH_FACT_CONV_RAW.period_min_key ,'HH24'),'2000-01-01 00:00:00'::timestamp) as period_hour_key,
     SUM(volume_in) as volume_in,
     SUM(volume_out) as volume_out
     FROM PROD.DWH_FACT_CONV_RAW
     GROUP BY 1,2) CONVtable

     ON SDRtable.subscriber_key = CONVtable.subscriber_key
     and SDRtable.hour = CONVtable.period_hour_key

GROUP BY 1,3,4
ORDER BY 3,4 ASC;

