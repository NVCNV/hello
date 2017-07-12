#!/bin/bash

TAKING_DATE=$1
TAKING_HOUR=$2
HIVEDB=$3
ORACLEDB=$4

HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/${HIVEDB}.db"
LOCAL_ADDR="/dt/tmpdata"
ORACLE_ADDR="userid=scott/tiger@${ORACLEDB}"
LOG_ADDR="/dt/sqllog"
CTL_ADDR="/dt/ctl"
HIVE_TBLES="volte_user_data gt_pulse_detail gt_pulse_cell_min gt_pulse_cell_base60 gt_pulse_detail_base60 gt_pulse_load_balence60"

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
exit 0