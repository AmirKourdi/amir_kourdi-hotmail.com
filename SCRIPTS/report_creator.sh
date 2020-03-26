#!/bin/bash
#author          : Amir Kourdi
#date            :2019112019
#version         :0.1
#name            :report creator.sh
# ----------------------------------------- #




#declare target_table

declare report_time=$(date "+%Y-%m-%d")



# MAIN

#main()
#{
#	/opt/vertica/bin/vsql -U dbadmin -w dbadmin -f /home/dbadmin/MCC_REPORT/SQL/mcc_hourly.sql -o /home/dbadmin/MCC_REPORT/OUTPUT/HOUR/mmc_hourly_report__$report_time.csv
#}


# RUN


#main()


/opt/vertica/bin/vsql -U dbadmin -w dbadmin -f /home/dbadmin/MCC_REPORT/SQL/mcc_hourly.sql -o /home/dbadmin/MCC_REPORT/OUTPUT/HOUR/mmc_hourly_report_$report_time.csv

