#!/bin/bash

export HADOOP_CONF_DIR=/opt/app/hdconf

ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3
OracleADDR=$4
VAADDR=$5
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_user_data/dt=${ANALY_DATE}/h=${ANALY_HOUR} 2 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_pulse_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} 4 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_pulse_cell_min/dt=${ANALY_DATE}/h=${ANALY_HOUR} 5 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_pulse_cell_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 6 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_pulse_detail_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 7 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_pulse_load_balence60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 33 2
