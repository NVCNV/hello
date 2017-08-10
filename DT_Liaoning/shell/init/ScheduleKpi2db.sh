#!/bin/bash

export HADOOP_CONF_DIR=/opt/app/hdconf

ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3
OracleADDR=$4
SCRIPT_ADDR=$5
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db
sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 1 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 3 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 5 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/ue_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 7 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 9 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 11 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 13 2

sh ${SCRIPT_ADDR}/HDFS2DB.sh  ${OracleADDR}  ${DIR}/business_type_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} 14 2
