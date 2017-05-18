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

#table cols
COL_NUM=$3TABLE=$2
#map num
MAP_NUM=$4

#columns
KPI_IMSI_COLS="imsi,imei,msisdn,cellid,ttime,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltemctimey,voltevdtime,voltevdtimey,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,voltesucc,srvccsuccS1"

KPI_CELL_COLS='ttime,cellid,voltemcsucc,voltemcatt,voltevdsucc,voltevdatt,voltetime,voltemctime,voltemctimey,voltevdtime,voltevdtimey,voltemchandover,volteanswer,voltevdhandover,voltevdanswer,srvccsucc,srvccatt,srvcctime,lteswsucc,lteswatt,srqatt,srqsucc,tauatt,tausucc,rrcrebuild,rrcsucc,rrcreq,imsiregatt,imsiregsucc,wirelessdrop,wireless,eabdrop,eab,eabs1swx,eabs1swy,s1tox2swx,s1tox2swy,enbx2swx,enbx2swy,uuenbswx,uuenbswy,uuenbinx,uuenbiny,swx,swy,attachx,attachy,voltesucc,srvccsuccS1'

EVENT_MSG='event_name,procedurestarttime,imsi,proceduretype,etype,cellid,targetcellid,falurecause,celltype,cellregion,cellkey,interface,prointerface,rangetime,ELONG,ELAT,EUPORDOWN'

KPI_MR_IMSI_COLS='imsi,imei,msisdn,cellid,rruid,gridid,ttime,dir_state,elong,elat,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty'

KPI_MR_CELL_COLS='cellid,ttime,dir_state,avgrsrpx,commy,avgrsrqx,ltecoverratex,weakcoverratex,overlapcoverratex,overlapcoverratey,upsigrateavgx,upsigrateavgy,updiststrox,updiststroy,model3diststrox,model3diststroy,uebootx,uebooty'

CELL_HOUR_HTTP_COLS="ttime,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

CELL_DAY_HTTP_COLS="ttime,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

IMSI_CELL_DAY_HTTP_COLS="ttime,imsi,msisdn,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

IMSI_CELL_HOUR_HTTP_COLS="ttime,imsi,msisdn,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

SGW_DAY_HTTP_COLS="ttime,sgw,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

SGW_HOUR_HTTP_COLS="ttime,sgw,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

SP_DAY_HTTP_COLS="ttime,sp,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

SP_HOUR_HTTP_COLS="ttime,sp,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

T_XDR_EVENT_MSG_COLS="city,xdrid,PROCEDURESTARTTIME,TTIME,procedureendtime,IMSI,IMEI,TEtac,msisdn,CELLID,sgw,sp,apptype,appsubtype,appstatus,etype,failcause,CELLREGION"

TAC_DAY_HTTP_COLS="ttime,tac,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

TAC_HOUR_HTTP_COLS="ttime,tac,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

UE_DAY_HTTP_COLS="ttime,imsi,msisdn,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

UE_HOUR_HTTP_COLS="ttime,imsi,msisdn,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsquerysucc,dnsqueryatt,tcpsetupsucc,tcpsetupreq,bearerultcpretransmit,bearerultcptransmit,bearerdltcpretransmit,bearerdltcptransmit,bearerultcpmissequence,bearerdltcpmissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,serviceimsucc,serviceimreq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime"

BUSINESS_TYPE_DETAIL="TTIME,city,region,CELLID,app_type,app_sub_type,uldata,dldata"

if [ $COL_NUM = 1 ]
then
   COLS=$KPI_IMSI_COLS
elif [ $COL_NUM = 2 ]; then
   COLS=$KPI_CELL_COLS
elif [ $COL_NUM = 3 ]; then
   COLS=$KPI_MR_IMSI_COLS
elif [ $COL_NUM = 4 ]; then
   COLS=$KPI_MR_CELL_COLS
elif [ $COL_NUM = 5 ]; then
  COLS=$CELL_HOUR_HTTP_COLS
   TABLE=cell_hour_http
elif [ $COL_NUM = 6 ]; then
   COLS=$CELL_DAY_HTTP_COLS
   TABLE=cell_day_http
elif [ $COL_NUM = 7 ]; then
   COLS=$IMSI_CELL_DAY_HTTP_COLS
   TABLE=imsi_cell_day_http
elif [ $COL_NUM = 8 ]; then
   COLS=$IMSI_CELL_HOUR_HTTP_COLS
   TABLE=imsi_cell_hour_http
elif [ $COL_NUM = 9 ]; then
   COLS=$SGW_DAY_HTTP_COLS
   TABLE=sgw_day_http
elif [ $COL_NUM = 10 ]; then
   COLS=$SGW_HOUR_HTTP_COLS
   TABLE=sgw_hour_http
elif [ $COL_NUM = 11 ]; then
   COLS=$SP_DAY_HTTP_COLS
   TABLE=sp_day_http
elif [ $COL_NUM = 12 ]; then
   COLS=$SP_HOUR_HTTP_COLS
   TABLE=sp_hour_http
elif [ $COL_NUM = 13 ]; then
   COLS=$T_XDR_EVENT_MSG_COLS
   TABLE=t_xdr_event_msg
elif [ $COL_NUM = 14 ]; then
   COLS=$TAC_DAY_HTTP_COLS
   TABLE=tac_day_http
elif [ $COL_NUM = 15 ]; then
   COLS=$TAC_HOUR_HTTP_COLS
   TABLE=tac_hour_http
elif [ $COL_NUM = 16 ]; then
   COLS=$UE_DAY_HTTP_COLS
   TABLE=imsi_day_http
elif [ $COL_NUM = 17 ]; then
   COLS=$UE_HOUR_HTTP_COLS
   TABLE=imsi_hour_http
elif [ $COL_NUM = 18 ]; then
   COLS=$BUSINESS_TYPE_DETAIL
   TABLE=business_type_detail
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
