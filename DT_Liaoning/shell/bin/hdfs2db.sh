#!/bin/sh
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/opt/app/sqoop/bin

#oracle jdbc url
#URL=jdbc:oracle:thin:@192.168.3.14:1521:umorpho
URL=jdbc:oracle:thin:@172.30.4.159:1521:m98v520

#oracle username
USERNAME=scott
#oracle passwd
PASSWD=tiger
#export hdfs dir
HDFS_DIR=$1
#oracel table
TABLE=$2
#table cols
COL_NUM=$3
#map num
MAP_NUM=$4

#columns
KPI_IMSI_COLS=imsi,imei,msisdn,cellid,ttime,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime 

KPI_CELL_COLS='ttime,cellid,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltevdtime,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,pagereq,pageresp,pageshowtimeall,pageresptimeall,pageshowsucc,httpdownflow,httpdowntime,mediareq,mediasucc,mediadownflow,mediadowntime'

EVENT_MSG='event_name,procedurestarttime,imsi,proceduretype,etype,cellid,targetcellid,falurecause,celltype,cellregion,cellkey,interface,prointerface,rangetime'

KPI_MR_IMSI_COLS='imsi,imei,msisdn,cellid,rruid,gridid,ttime,dir_state,elong,elat,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty'

KPI_MR_CELL_COLS='cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty'

if [ $COL_NUM = 1 ] 
then
   COLS=$KPI_IMSI_COLS
elif [ $COL_NUM = 2 ]; then
   COLS=$KPI_CELL_COLS
elif [ $COL_NUM = 3 ]; then
   COLS=$KPI_MR_IMSI_COLS
elif [ $COL_NUM = 4 ]; then
   COLS=$KPI_MR_CELL_COLS
else 
   COLS=$EVENT_MSG
fi
/opt/app/sqoop/bin/sqoop export --connect $URL \
--username $USERNAME --password tiger \
--table $TABLE \
--columns $COLS \
--export-dir $HDFS_DIR \
--fields-terminated-by ',' \
--m $MAP_NUM \
--input-null-string '\\N' --input-null-non-string '\\N' 
