SELECT
day_date as DATE,
period_hour_key as HOUR,
a.application_key as APPLICATION,
application_group_key as APPLICATION_GROUP,
subscriber_key as MSISDN,
sgsn_mcc_mnc, 
rtt_internal_avg_msec,           
rtt_external_avg_msec,
volume_in as DOWNLOAD,
volume_out as UPLOAD, 
total_volume as TOATAL,
new_connections,
live_connections,
number_of_sessions
FROM
	prod.dwh_faCT_coNV_aGG_hOUR a 
	inner join
	prod.dwh_dim_apPLICATIONS b
				
	ON a.application_key = b.application_key
	where a.period_hour_key between trunc(sysdate)-1 and trunc(sysdate)
	AND sgsn_mcc_mnc IS NOT NULL;