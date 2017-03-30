#!/bin/bash
ANALY_DATE=$1
ANALY_HOUR=$2
input_dir=hdfs://dtcluster/user/hive/warehouse/dcl.db
yingcai_input_dir=hdfs://dtcluster/datang/
s1mme=${yingcai_input_dir}/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}
x2=${input_dir}/tb_xdr_ifc_x2/dt=${ANALY_DATE}/h=${ANALY_HOUR}
uu=${input_dir}/tb_xdr_ifc_uu/dt=${ANALY_DATE}/h=${ANALY_HOUR}
gxrx=${yingcai_input_dir}/volte_rx/${ANALY_DATE}/${ANALY_HOUR}
mrosorce=${input_dir}/lte_mro_source/dt=${ANALY_DATE}/h=${ANALY_HOUR}
http=${yingcai_input_dir}/s1u_http_orgn/${ANALY_DATE}/${ANALY_HOUR}
mw=${yingcai_input_dir}/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}
xdr_output_dir=hdfs://dtcluster/output/xdr
mrosorce_output_dir=hdfs://dtcluster/output/mrosource

time hadoop jar /dt/lib/dthbase01.jar cn.com.dtmobile.xdr.job.ImportCommonXdrDataJob $s1mme $x2 $uu $gxrx $mrosorce $http $mw $xdr_output_dir -l true -c true
time hadoop jar /dt/lib/dthbase01.jar cn.com.dtmobile.xdr.job.ImportXdlDataJob $mrosorce $mrosorce_output_dir -l true -c true 8
