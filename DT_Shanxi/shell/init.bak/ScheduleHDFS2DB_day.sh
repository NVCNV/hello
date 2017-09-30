#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
DATABASE=$2
OracleADDR=$3
VAADDR=$4
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db


sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_freq_baseday/dt=${ANALY_DATE} 8 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_balence_baseday/dt=${ANALY_DATE} 28 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_shorttimelen_baseday/dt=${ANALY_DATE} 29 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_overtimelen_baseday/dt=${ANALY_DATE} 30 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_commusermore_baseday/dt=${ANALY_DATE} 31 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/gt_highattach_baseday/dt=${ANALY_DATE} 32 2