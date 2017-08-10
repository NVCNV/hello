#!/bin/bash

export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/opt/app/sqoop/bin

#oracle jdbc url
URL=jdbc:oracle:thin:@$1
#oracle username
USERNAME=scott
#oracle passwd
PASSWD=tiger
#export hdfs dir
HDFS_DIR=$2
#table cols
COL_NUM=$3
#map num
MAP_NUM=$4
#columns
tac_hour_http='ttime,tac,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

tac_day_http='ttime,tac,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

cell_hour_http='ttime,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

cell_day_http='ttime,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

sp_hour_http='ttime,sp,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

sp_day_http='ttime,sp,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

ue_hour_http='ttime,imsi,msisdn,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

ue_day_http='ttime,imsi,msisdn,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

imsi_cell_hour_http='ttime,imsi,msisdn,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

imsi_cell_day_http='ttime,imsi,msisdn,cellid,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

sgw_hour_http='ttime,sgw,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

sgw_day_http='ttime,sgw,browsedownloadvisits,videoservicevisits,instantmessagevisits,appvisits,browsedownloadbusiness,videoservicebusiness,instantmessagebusiness,appbusiness,dnsQuerySucc,dnsQueryAtt,tcpSetupSucc,tcpSetupReq,BearerULTCPRetransmit,BearerULTCPTransmit,BearerDLTCPRetransmit,BearerDLTCPTransmit,BearerULTCPMissequence,BearerDLTCPMissequence,pageresp,pagereq,pageresptimeall,pageshowsucc,pageshowtimeall,httpdownflow,httpdowntime,mediasucc,mediareq,mediadownflow,mediadowntime,ServiceIMSucc,ServiceIMReq,readvisits,wbvisits,navigationvisits,musicvisits,gamevisits,payvisits,Animevisits,mailvisits,p2pvisits,voipvisits,MultimediaMsgvisits,financialvisits,securityvisits,shoppingvisits,travelvisits,cloudstoragevisits,othervisits,readbusiness,wbbusiness,navigationbusiness,musicbusiness,gamebusiness,paybusiness,Animebusiness,mailbusiness,p2pbusiness,voipbusiness,MultimediaMsgbusiness,financialbusiness,securitybusiness,shoppingbusiness,travelbusiness,cloudstoragebusiness,otherbusiness,mediaRespTimeall,mediaResp,ServiceIMTrans,ServiceIMFlow,ServiceIMTime'

t_xdr_event_msg='city,xdrid,procedurestarttime,ttime,procedureendtime,imsi,imei,tetac,msisdn,cellid,sgw,sp,apptype,appsubtype,appstatus,etype,FAILCAUSE,cellregion'

business_type_detail='TTIME,city,region,CELLID,app_type,app_sub_type,uldata,dldata'

if [ ${COL_NUM} = 1 ] ; then
    COLS=${tac_hour_http}
    TABLE=tac_hour_http
elif [ ${COL_NUM} = 2 ] ; then
    COLS=${tac_day_http}
    TABLE=tac_day_http
elif [ ${COL_NUM} = 3 ] ; then
    COLS=${cell_hour_http}
    TABLE=cell_hour_http
elif [ ${COL_NUM} = 4 ] ; then
    COLS=${cell_day_http}
    TABLE=cell_day_http
elif [ ${COL_NUM} = 5 ] ; then
    COLS=${sp_hour_http}
    TABLE=sp_hour_http
elif [ ${COL_NUM} = 6 ] ; then
    COLS=${sp_day_http}
    TABLE=sp_day_http
elif [ ${COL_NUM} = 7 ] ; then
    COLS=${ue_hour_http}
    TABLE=imsi_hour_http
elif [ ${COL_NUM} = 8 ] ; then
    COLS=${ue_day_http}
    TABLE=imsi_day_http
elif [ ${COL_NUM} = 9 ] ; then
    COLS=${imsi_cell_hour_http}
    TABLE=imsi_cell_hour_http
elif [ ${COL_NUM} = 10 ] ; then
    COLS=${imsi_cell_day_http}
    TABLE=imsi_cell_day_http
elif [ ${COL_NUM} = 11 ] ; then
    COLS=${sgw_hour_http}
    TABLE=sgw_hour_http
elif [ ${COL_NUM} = 12 ] ; then
    COLS=${sgw_day_http}
    TABLE=sgw_day_http
elif [ ${COL_NUM} = 13 ] ; then
    COLS=${t_xdr_event_msg}
    TABLE=t_xdr_event_msg
else
    COLS=${business_type_detail}
    TABLE=business_type_detail
fi

sqoop export --connect $URL \
--username $USERNAME \
--password $PASSWD \
--table $TABLE \
--columns $COLS \
--export-dir $HDFS_DIR \
--input-fields-terminated-by ',' \
--m $MAP_NUM \
--input-null-string '\\N' \
--input-null-non-string '\\N'