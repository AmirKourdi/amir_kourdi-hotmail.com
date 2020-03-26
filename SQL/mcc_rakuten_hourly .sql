SELECT
CASE
   WHEN sgsn_mcc_mnc IN ('44011') THEN 'Rakuten (jp)'
   ELSE 'OTHER'
END AS NETWORK,
sgsn_mcc_mnc as MCC_MNC,
COUNT(distinct subscriber_key) as NUMBER_OF_MSISDN ,
day_date as DATE,
period_hour_key AS PERIOD_HOUR,
CAST(SUM(volume_in/1024/1024/1024) AS DECIMAL(100,2)) as VOLUME_IN_GB,
CAST(SUM(volume_out/1024/1024/1024) AS DECIMAL(100,2)) as VOLUME_OUT_GB,
CAST((SUM(volume_in/1024/1024/1024) + SUM(volume_out/1024/1024/1024)) AS DECIMAL(100,2)) as TOTAL_VOLUME_GB

FROM  PROD.DWH_FACT_CONV_AGG_HOUR

WHERE to_char(day_date,'YYYY-MM-DD') = to_char(getdate(),'YYYY-MM-DD')
and sgsn_mcc_mnc IN ('44011')
GROUP BY 2,4,5
ORDER BY 4,5 ASC;



