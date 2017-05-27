#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
TableName=$3
DIR=hdfs://dtcluster/user/hive/warehouse/${TableName}.db

sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_user_data/dt=${ANALY_DATE}/h=${ANALY_HOUR} 2 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/volte_gtuser_data/dt=${ANALY_DATE}/h=${ANALY_HOUR} 3 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_pulse_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} 4 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_pulse_cell_min/dt=${ANALY_DATE}/h=${ANALY_HOUR} 5 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_pulse_cell_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 6 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_pulse_detail_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 7 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/TB_XDR_IFC_UU/dt=${ANALY_DATE}/h=${ANALY_HOUR} 9 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/LTE_MRO_SOURCE/dt=${ANALY_DATE}/h=${ANALY_HOUR} 10 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/TB_XDR_IFC_GMMWMGMIMJISC/dt=${ANALY_DATE}/h=${ANALY_HOUR} 11 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/warnningtable/dt=${ANALY_DATE}/h=${ANALY_HOUR} 24 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} 25 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/LTECELL/dt=${ANALY_DATE}/h=${ANALY_HOUR} 26 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_capacity_config/dt=${ANALY_DATE}/h=${ANALY_HOUR} 27 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_pulse_load_balence60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 32 2

sh VolumeAnalyseHDFS2db.sh ${DIR}/gt_balence_pair/ 33 2

