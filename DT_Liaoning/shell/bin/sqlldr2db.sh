#!/bin/bash
export ORALCE_HOME=/opt/app/oracle
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
export PATH=$PATH:$LD_LIBRARY_PATH
TAKING_DATE=$1
TAKING_HOUR=$2
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
HDFS_ADDR="hdfs://dtcluster/user/hive/warehouse/dcl.db"
DB_ADDR="userid=scott/tiger@umorpho602"
HIVE_TBLES="business_type_detail cell_hour_http imsi_cell_hour_http mr_gt_cell_ana_base60 mr_gt_user_ana_base60 sgw_hour_http sp_hour_http t_xdr_event_msg tac_hour_http volte_gt_cell_ana_base60 volte_gt_user_ana_base60 zc_city_data"

LOCALDIR="/dt/bin/data"
CTLDIR="/dt/bin/ctl"
LOG_DIR=/dt/bin/sqlldrLog

rm -rf ${LOCALDIR}/$TAKING_DATE
mkdir ${LOCALDIR}/$TAKING_DATE

for tableName in ${HIVE_TBLES}
do
#mkdir -p ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}
mkdir -p ${LOG_DIR}/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
echo "get from ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* to ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}"
hdfs dfs -getmerge ${HDFS_ADDR}/${tableName}/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/${tableName}.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat log=/dt/bin/sqlldrLog/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
done

mkdir -p ${LOG_DIR}/t_event_msg/${TAKING_DATE}/${TAKING_HOUR}
hdfs dfs -getmerge ${HDFS_ADDR}/exception_analysis/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/t_event_msg.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/t_event_msg.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/t_event_msg.dat log=${LOG_DIR}/t_event_msg/${TAKING_DATE}/${TAKING_HOUR}

mkdir -p ${LOG_DIR}/imsi_hour_http/${TAKING_DATE}/${TAKING_HOUR}
hdfs dfs -getmerge ${HDFS_ADDR}/ue_hour_http/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/imsi_hour_http.dat
sqlldr ${DB_ADDR} control=${CTLDIR}/imsi_hour_http.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/imsi_hour_http.dat log=${LOG_DIR}/imsi_hour_http/${TAKING_DATE}/${TAKING_HOUR}
exit

