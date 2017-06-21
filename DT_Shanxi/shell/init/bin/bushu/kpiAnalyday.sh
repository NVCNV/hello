#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1
DB=$2
echo $DB

echo "CRH_cell_day_KPI_new.sh ${ANALY_DATE} ${DB}"
./CRH_cell_day_KPI_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_IMSI_cell_day_KPI_new.sh ${ANALY_DATE} ${DB}"
./CRH_IMSI_cell_day_KPI_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_MR_cell_day_kpi_new.sh ${ANALY_DATE} ${DB}"
./CRH_MR_cell_day_kpi_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_MR_cell_grid_day_kpi_new.sh ${ANALY_DATE} ${DB}"
./CRH_MR_cell_grid_day_kpi_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_MR_imsi_cell_grid_day_kpi_new.sh ${ANALY_DATE} ${DB}"
./CRH_MR_imsi_cell_grid_day_kpi_new.sh ${ANALY_DATE} ${DB} &
wait
echo "CRH_export_cell_day_KPI_new.sh"    
./CRH_export_cell_day_KPI_new.sh ${ANALY_DATE} ${DB} &   
echo "CRH_export_imsi_cell_day_KPI_new.sh"
./CRH_export_imsi_cell_day_KPI_new.sh ${ANALY_DATE} ${DB} &    
echo "CRH_export_mr_cell_day_kpi_new.sh" 
./CRH_export_mr_cell_day_kpi_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_export_mr_cell_grid_day_KPI_new.sh"
./CRH_export_mr_cell_grid_day_KPI_new.sh ${ANALY_DATE} ${DB} &
echo "CRH_export_mr_imsi_cell_grid_day_KPI_new.sh"
./CRH_export_mr_imsi_cell_grid_day_KPI_new.sh ${ANALY_DATE} ${DB} &

wait
#echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_user_ana_baseday volte_gt_user_ana_baseda 14 2"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_user_ana_baseday/dt=${ANALY_DATE} volte_gt_user_ana_baseday 14 2 
#echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_cell_ana_baseday volte_gt_cell_ana_baseday 12 2"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_cell_ana_baseday/dt=${ANALY_DATE} volte_gt_cell_ana_baseday 12 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_user_ana_baseday mr_gt_user_ana_baseday 18 2"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_user_ana_baseday/dt=${ANALY_DATE} mr_gt_user_ana_baseday 18 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_cell_ana_baseday mr_gt_cell_ana_baseday 16 2"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_cell_ana_baseday/dt=${ANALY_DATE} mr_gt_cell_ana_baseday 16 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_cell_ana_baseday mr_gt_cell_ana_base60 34 2"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_grid_ana_baseday/dt=${ANALY_DATE} mr_gt_grid_ana_baseday 34 2

