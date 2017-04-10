#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
SOURCE_DB=dcl.db
#Kpi Day to Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_baseday volte_gt_user_ana_baseday 1 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_baseday/dt=${ANALY_DATE} volte_gt_user_ana_baseday_t 1 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_baseday volte_gt_cell_ana_baseday 2 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} volte_gt_cell_ana_baseday_t 2 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_baseday mr_gt_user_ana_baseday 3 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_baseday/dt=${ANALY_DATE} mr_gt_user_ana_baseday_t 3 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_baseday mr_gt_cell_ana_baseday 4 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_baseday/dt=${ANALY_DATE} mr_gt_cell_ana_baseday_t 4 4

#Kpi Hour To Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_base60 volte_gt_user_ana_base60 1 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_user_ana_base60_t 1 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_base60 volte_gt_cell_ana_base60 2 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_cell_ana_base60_t 2 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_base60 mr_gt_user_ana_base60 3 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_user_ana_base60_t 3 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_base60 mr_gt_cell_ana_base60 4 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_cell_ana_base60_t 4 4

exception to Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/exception_analysis t_event_msg 5 4"
/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR} t_event_msg_t 5 4

