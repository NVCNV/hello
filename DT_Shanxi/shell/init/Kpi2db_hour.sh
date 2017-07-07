#!/bin/bash
export ORALCE_HOME=/opt/oracle
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export PATH=$PATH:$LD_LIBRARY_PATH

TAKING_DATE=$1
TAKING_HOUR=$2
HIVEDB=$3
ORACLEDB=$4
mypath="$("$(dirname; "$0")";pwd)"
cd $mypath

HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/${HIVEDB}.db"
LOCAL_ADDR='/dt/NewData'
ORACLE_ADDR="userid=scott/tiger@${ORACLEDB}"
LOG_ADDR='/dt/sqllog'
CTL_ADDR='/dt/ctl'
HIVE_TBLES='t_xdr_event_msg business_type_detail tac_hour_http cell_hour_http sp_hour_http ue_hour_http imsi_cell_hour_http sgw_hour_http zc_city_data lte_cellmr_source'

rm -rf ${LOCAL_ADDR}/${TAKING_DATE}
mkdir ${LOCAL_ADDR}/${TAKING_DATE}

for tableName in ${HIVE_TBLES}
do
	mkdir -p ${LOG_ADDR}/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
	echo "getmerge from ${HDFS_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/ to ${LOCAL_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat"
	hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCAL_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat
	echo "load ${LOCAL_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat to  oralce ${tableName}"
	sqlldr ${ORACLE_ADDR} control=${CTL_ADDR}/${tableName}.ctl data=${LOCAL_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat log=${LOG_ADDR}/${tableName}/${TAKING_DATE}/${TAKING_HOUR}/${TAKING_DATE}_${TAKING_HOUR}.log
done