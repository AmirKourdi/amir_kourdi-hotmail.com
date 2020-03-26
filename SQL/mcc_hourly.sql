SELECT
     sgsn_mcc_mnc,
     COUNT(distinct subscriber_key) as number_of_subscribers,
     day_date,
     period_hour_key,
     CAST(SUM(volume_in/1024/1024) AS DECIMAL(100,2)) as volume_in_MB,
     CAST(SUM(volume_out/1024/1024) AS DECIMAL(100,2)) as volume_out_MB,
     CAST((SUM(volume_in/1024/1024) + SUM(volume_out/1024/1024)) AS DECIMAL(100,2)) as total_volume_MB
FROM  PROD.DWH_FACT_CONV_AGG_HOUR
WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate(),'YYYY-MM-DD')
GROUP BY 1,3,4
ORDER BY 1,2 ASC;
