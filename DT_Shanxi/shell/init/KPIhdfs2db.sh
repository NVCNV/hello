#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3
OracleADDR=$4
VAADDR=$5
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db


#Kpi Hour To Oracle
#echo"sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 35 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 35 2 &

#echo"sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 36 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 36 2 &

#echo"sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 38 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 38 2 &

#echo"sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 39 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 39 2 &

#echo"sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/t_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 25 2"
#sh VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/t_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 37 2 &

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 25 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/business_type_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} 1 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 12 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 14 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 16 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/ue_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 18 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 20 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} 22 2

sh ${VAADDR}/VolumeAnalyseHDFS2db.sh ${OracleADDR} ${DIR}/zc_city_data/dt=${ANALY_DATE}/h=${ANALY_HOUR} 50 2
