#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
SOURCE_DB=dcl.db
DIR=hdfs://dtcluster/user/hive/warehouse/dcl.db
#Kpi Hour To Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_base60 volte_gt_user_ana_base60 1 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_user_ana_base60 1 4 &
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_base60 volte_gt_cell_ana_base60 2 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_cell_ana_base60 2 4 &
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_base60 mr_gt_user_ana_base60 3 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_user_ana_base60 3 4 &
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_base60 mr_gt_cell_ana_base60 4 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_cell_ana_base60 4 4 &
wait
#exception to Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/exception_analysis t_event_msg 523 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR} t_event_msg 1312 4

sh /dt/bin/hdfs2db.sh ${DIR}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} cell_hour_http 5 2

#sh /dt/bin/hdfs2db.sh ${DIR}/cell_day_http/dt=${ANALY_DATE} cell_day_http 6 2

#sh /dt/bin/hdfs2db.sh ${DIR}/imsi_cell_day_http/dt=${ANALY_DATE} imsi_cell_day_http 7 2

sh /dt/bin/hdfs2db.sh ${DIR}/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} imsi_cell_hour_http 8 2

#sh /dt/bin/hdfs2db.sh ${DIR}/sgw_day_http/dt=${ANALY_DATE} sgw_day_http 9 2

sh /dt/bin/hdfs2db.sh ${DIR}/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} sgw_hour_http 10 2

#sh /dt/bin/hdfs2db.sh ${DIR}/sp_day_http/dt=${ANALY_DATE} sp_day_http 11 2

sh /dt/bin/hdfs2db.sh ${DIR}/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} sp_hour_http 12 2

sh /dt/bin/hdfs2db.sh ${DIR}/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} t_xdr_event_msg 13 2

#sh /dt/bin/hdfs2db.sh ${DIR}/tac_day_http/dt=${ANALY_DATE} tac_day_http 14 2

sh /dt/bin/hdfs2db.sh ${DIR}/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} tac_hour_http 15 2

#sh /dt/bin/hdfs2db.sh ${DIR}/ue_day_http/dt=${ANALY_DATE} imsi_day_http 16 2

sh /dt/bin/hdfs2db.sh ${DIR}/ue_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} imsi_hour_http 17 2

sh /dt/bin/hdfs2db.sh ${DIR}/business_type_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} business_type_detail 18 4
