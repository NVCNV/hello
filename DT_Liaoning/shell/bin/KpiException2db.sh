#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
ANALY_HOUR=$2
SOURCE_DB=dcl.db
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_user_ana_base60 volte_gt_user_ana_base60 1 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_user_ana_base60 1 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_cell_ana_base60 volte_gt_cell_ana_base60 2 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_cell_ana_base60 2 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_user_ana_base60 mr_gt_user_ana_base60 3 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_user_ana_base60 3 4
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_cell_ana_base60 mr_gt_cell_ana_base60 4 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_cell_ana_base60 4 4

#exception to Oracle
echo "/dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/exception_analysis t_event_msg 5 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR} t_event_msg 5 4
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/cell_hour_http/ cell_hour_http 5 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} cell_hour_http 5 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/imsi_cell_hour_http/ imsi_cell_hour_http 8 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/imsi_cell_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} imsi_cell_hour_http 8 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/sgw_hour_http/ sgw_hour_http 10 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/sgw_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} sgw_hour_http 10 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/sp_hour_http/ sp_hour_http 12 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/sp_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} sp_hour_http 12 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/t_xdr_event_msg/ t_xdr_event_msg 13 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/t_xdr_event_msg/dt=${ANALY_DATE}/h=${ANALY_HOUR} t_xdr_event_msg 13 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/tac_hour_http/ tac_hour_http 15 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/tac_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} tac_hour_http 15 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/ue_hour_http/ imsi_hour_http 17"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/ue_hour_http/dt=${ANALY_DATE}/h=${ANALY_HOUR} imsi_hour_http 17
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/business_type_detail/ business_type_detail 18 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/business_type_detail/dt=${ANALY_DATE}/h=${ANALY_HOUR} business_type_detail 18 4
