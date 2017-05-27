#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
DATABASE=$2
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db

sh VolumeAnalyseHDFS2db.sh ${DIR}/tac_day_http/dt=${ANALY_DATE} 13 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/cell_day_http/dt=${ANALY_DATE} 15 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sp_day_http/dt=${ANALY_DATE} 17 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/ue_day_http/dt=${ANALY_DATE} 19 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/imsi_cell_day_http/dt=${ANALY_DATE} 21 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/sgw_day_http/dt=${ANALY_DATE} 23 2