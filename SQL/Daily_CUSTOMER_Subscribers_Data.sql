SELECT
period_day_key as DAY,
'CUSTOMER' as Channel,
CONV.application_key as APPLICATION,
application_group_key as APPLICATION_GROUP,
CONV.subscriber_key as MSISDN,
host_internal_key,
sgsn_mcc_mnc, 
rtt_internal_avg_msec,           
rtt_external_avg_msec,
volume_in as DOWNLOAD,
volume_out as UPLOAD, 
total_volume as TOATAL,
new_connections,
live_connections,
number_of_sessions,
session_duration,
activity_time
FROM
	prod.dwh_faCT_coNV_aGG_day CONV 
	inner join
	prod.dwh_dim_apPLICATIONS APPGROUP 
				
	ON CONV.application_key = APPGROUP.application_key
	
	inner join
	prod.dwh_dim_suBSCRIBERs DIMSUB
	
	ON CONV.subscriber_key = DIMSUB.subscriber_key
	
where sgsn_mcc_mnc IS NOT NULL  and CONV.application_key = 'Facebook' AND period_day_key = '2020-02-22 00:00:00' and subscriber_reserved_1 = 'CUSTOMER' and CONV.subscriber_key ='817084262626';

