#!/bin/bash

TAKING_DATE=$1
HIVEDB=$2
ORACLEDB=$3


HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/${HIVEDB}.db"
LOCAL_ADDR='/dt/NewData'
ORACLE_ADDR="userid=scott/tiger@${ORACLEDB}"
LOG_ADDR='/dt/sqllog'
CTL_ADDR='/dt/ctl'
HIVE_TBLES='tac_day_http cell_day_http sp_day_http ue_day_http imsi_cell_day_http sgw_day_http'

rm -rf ${LOCAL_ADDR}/${TAKING_DATE}
mkdir ${LOCAL_ADDR}/${TAKING_DATE}

for tableName in ${HIVE_TBLES}
do
	mkdir -p ${LOG_ADDR}/${tableName}/${TAKING_DATE}
	echo "getmerge from ${HDFS_ADDR}/${TAKING_DATE}/ to ${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat"
	hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/* ${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat
	echo "load ${LOCAL_ADDR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat to  oralce ${tableName}"
	sqlldr ${ORACLE_ADDR} control=${CTL_ADDR}/${tableName}.ctl data=${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat log=${LOG_ADDR}/${tableName}/${TAKING_DATE}/${TAKING_DATE}.log
done