#!/bin/bash
TAKING_DATE=$1
TAKING_HOUR=$2
HIVEDB=$3
ORACLEDB=$4
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/${HIVEDB}.db"
DB_ADDR="userid=scott/tiger@${ORACLEDB}"
HIVE_TBLES="business_type_detail cell_hour_http imsi_cell_hour_http mr_gt_cell_ana_base60 mr_gt_user_ana_base60 sgw_hour_http sp_hour_http t_xdr_event_msg tac_hour_http volte_gt_cell_ana_base60 volte_gt_user_ana_base60 zc_city_data"

LOCALDIR="/dt/tmpdata"
CTLDIR="/dt/ctl"
LOG_DIR="/dt/sqlldrLog"

rm -rf ${LOCALDIR}/$TAKING_DATE
mkdir ${LOCALDIR}/$TAKING_DATE

for tableName in ${HIVE_TBLES}
do
#mkdir -p ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}
mkdir -p ${LOG_DIR}/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
echo "get from ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* to ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}"
hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/${tableName}.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat log=/dt/sqlldrLog/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
done

mkdir -p ${LOG_DIR}/t_event_msg/${TAKING_DATE}/${TAKING_HOUR}
hdfs dfs -getmerge ${HDFS_ADDR}/exception_analysis/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/t_event_msg.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/t_event_msg.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/t_event_msg.dat log=${LOG_DIR}/t_event_msg/${TAKING_DATE}/${TAKING_HOUR}

mkdir -p ${LOG_DIR}/imsi_hour_http/${TAKING_DATE}/${TAKING_HOUR}
hdfs dfs -getmerge ${HDFS_ADDR}/ue_hour_http/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/imsi_hour_http.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/imsi_hour_http.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/imsi_hour_http.dat log=${LOG_DIR}/imsi_hour_http/${TAKING_DATE}/${TAKING_HOUR}
exit 0

