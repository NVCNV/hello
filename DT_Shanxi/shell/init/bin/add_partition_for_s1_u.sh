#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
DB_PATH=$3
#hdfs dfs -mkdir -p /user/hive/warehouse/shanxikpi.db/tb_xdr_ifc_gmmwmgmimjisc_new/dt=${ANALY_DATE}/h=${ANALY_HOUR}

#hdfs dfs -cp /datang/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/*mw* /user/hive/warehouse/shanxikpi.db/tb_xdr_ifc_gmmwmgmimjisc_new/dt=${ANALY_DATE}/h=${ANALY_HOUR}

hive<< EOF
USE s1_u;
alter table tb_xdr_ifc_s1u_common drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_common add partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
location "/${DB_PATH}/tb_ifc_common/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_dns drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_dns add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_dns/${ANALY_DATE}/${ANALY_HOUR}";


alter table tb_xdr_ifc_s1u_http drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_http add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_http/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_mms drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_mms add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_mms/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_ftp drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_ftp add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_ftp/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_email drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_email add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_email/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_voip drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_voip add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_voip/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_rtsp drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_rtsp add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_rtsp/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_p2p drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_p2p add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_p2p/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1u_rtcomm drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1u_rtcomm add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/${DB_PATH}/tb_xdr_ifc_rtcomm/${ANALY_DATE}/${ANALY_HOUR}";
 
EOF

