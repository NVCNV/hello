#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
DATABASE=$2
OracleADDR=$3
VAADDR=$4
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db

#echo"sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_user_ana_baseday/dt=${ANALY_DATE} 40 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_user_ana_baseday/dt=${ANALY_DATE} 40 2 &

#echo"sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} 36 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} 41 2 &

#echo"sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_user_ana_baseday/dt=${ANALY_DATE} 42 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_user_ana_baseday/dt=${ANALY_DATE} 42 2 &

#echo"sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_cell_ana_baseday/dt=${ANALY_DATE} 39 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_cell_ana_baseday/dt=${ANALY_DATE} 43 2 &

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/tac_day_http/dt=${ANALY_DATE} 13 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/cell_day_http/dt=${ANALY_DATE} 15 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/sp_day_http/dt=${ANALY_DATE} 17 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/ue_day_http/dt=${ANALY_DATE} 19 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/imsi_cell_day_http/dt=${ANALY_DATE} 21 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/sgw_day_http/dt=${ANALY_DATE} 23 2
