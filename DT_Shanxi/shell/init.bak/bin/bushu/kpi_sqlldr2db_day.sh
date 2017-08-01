#!/bin/bash

TAKING_DATE=$1
HIVEDB=$2
ORACLEDB=$3
HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/${HIVEDB}.db"
LOCAL_ADDR="/dt/tmpdata"
ORACLE_ADDR="userid=scott/tiger@${ORACLEDB}"
LOG_ADDR="/dt/sqllog"
CTL_ADDR="/dt/ctl"
HIVE_TBLES="volte_gt_user_ana_baseday volte_gt_cell_ana_baseday mr_gt_user_ana_baseday mr_gt_cell_ana_baseday mr_gt_grid_ana_baseday"

rm -rf ${LOCAL_ADDR}/${TAKING_DATE}
mkdir ${LOCAL_ADDR}/${TAKING_DATE}

for tableName in ${HIVE_TBLES}
do
	mkdir -p ${LOG_ADDR}/${tableName}/${TAKING_DATE}
	echo "getmerge from ${HDFS_ADDR}/${TAKING_DATE}/ to ${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat"
	hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/* ${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat
	echo "load ${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat to  oralce ${tableName}"
	sqlldr ${ORACLE_ADDR} control=${CTL_ADDR}/${tableName}.ctl data=${LOCAL_ADDR}/${TAKING_DATE}/${tableName}.dat log=${LOG_ADDR}/${tableName}/${TAKING_DATE}/${TAKING_DATE}.log
done
exit 0