#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
TableName=$3
DIR=hdfs://dtcluster/user/hive/warehouse/${TableName}.db

sh VolumeAnalyseHDFS2db.sh ${DIR}/business_type_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} 1 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 12 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 14 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 16 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/ue_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 18 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 20 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 22 2