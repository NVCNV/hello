#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
ANALY_DATE=$1
SOURCE_DB=dcl.db
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_user_ana_baseday volte_gt_user_ana_baseda 1 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_user_ana_baseday/dt=${ANALY_DATE} volte_gt_user_ana_baseday 1 4
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_cell_ana_baseday volte_gt_cell_ana_baseday 2 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} volte_gt_cell_ana_baseday 2 4
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_user_ana_baseday mr_gt_user_ana_baseday 3 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_user_ana_baseday/dt=${ANALY_DATE} mr_gt_user_ana_baseday 3 4
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_cell_ana_baseday mr_gt_cell_ana_baseday 4 4"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/mr_gt_cell_ana_baseday/dt=${ANALY_DATE} mr_gt_cell_ana_baseday 4 4
echo "sh /dt/bin/hdfs2db.shhdfs://dtcluster/user/hive/warehouse/dcl.db/imsi_cell_day_http imsi_cell_day_http 7 2"
sh /dt/bin/hdfs2db.shhdfs://dtcluster/user/hive/warehouse/dcl.db/imsi_cell_day_http/dt=${ANALY_DATE} imsi_cell_day_http 7 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/ue_day_http imsi_day_http 16 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/ue_day_http/dt=${ANALY_DATE} imsi_day_http 16 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/tac_day_http tac_day_http 14 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/tac_day_http/dt=${ANALY_DATE} tac_day_http 14 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/sp_day_http sp_day_http 11 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/sp_day_http/dt=${ANALY_DATE} sp_day_http 11 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/sgw_day_http sgw_day_http 9 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/sgw_day_http/dt=${ANALY_DATE} sgw_day_http 9 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/cell_day_http cell_day_http 6 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/cell_day_http/dt=${ANALY_DATE} cell_day_http 6 2
echo "sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/dcl.db/imsi_cell_day_http imsi_cell_day_http 7 2"
sh /dt/bin/hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${SOURCE_DB}/imsi_cell_day_http/dt=${ANALY_DATE} imsi_cell_day_http 7 2
