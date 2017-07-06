#!/bin/bash
export ORALCE_HOME=/opt/app/oracle
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export PATH=$PATH:$LD_LIBRARY_PATH
TAKING_DATE=$1
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/dcl.db"
DB_ADDR="userid=scott/tiger@umorpho602"
HIVE_TBLES="cell_day_http imsi_cell_day_http mr_gt_cell_ana_baseday mr_gt_user_ana_baseday sgw_day_http sp_day_http tac_day_http volte_gt_cell_ana_baseday volte_gt_user_ana_baseday"

LOCALDIR="/dt/bin/data/day"
CTLDIR="/dt/bin/ctl"
LOG_DIR=/dt/bin/sqlldrLog

rm -rf ${LOCALDIR}/$TAKING_DATE
mkdir ${LOCALDIR}/$TAKING_DATE

for tableName in ${HIVE_TBLES}
do
#mkdir -p ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}
mkdir -p ${LOG_DIR}/${tableName}/${TAKING_DATE}
echo "get from ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/* to ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}"
hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/${tableName}.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/${tableName}.ctl data=${LOCALDIR}/${TAKING_DATE}/${tableName}.dat log=/dt/bin/sqlldrLog/${tableName}/${TAKING_DATE}
done

mkdir -p ${LOG_DIR}/imsi_day_http/${TAKING_DATE}
hdfs dfs -getmerge ${HDFS_ADDR}/ue_day_http/dt=${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/imsi_day_http.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/imsi_day_http.ctl data=${LOCALDIR}/${TAKING_DATE}/imsi_day_http.dat log=${LOG_DIR}/imsi_day_http/${TAKING_DATE}
exit
