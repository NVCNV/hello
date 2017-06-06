#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3
DEFAULTDB=$4
#echo "cell_day_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
#./cell_day_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR} &
echo "CRH_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
./CRH_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &
#echo "imsi_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
#./imsi_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} &
echo "CRH_IMSI_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
./CRH_IMSI_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &
#echo "mr_cell_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
#./mr_cell_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} &
echo "CRH_MR_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
./CRH_MR_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &
#echo "mr_imsi_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
#./mr_imsi_day_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} &
echo "CRH_MR_imsi_cell_grid_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
./CRH_MR_imsi_cell_grid_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &
#CRH_MR_imsi_cell_grid_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB}
echo "CRH_MR_cell_grid_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR}"
./CRH_MR_cell_grid_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &  
#echo "export_cell_day_kpi.sh"    
#./export_cell_day_kpi.sh 
wait   
echo "CRH_export_cell_hour_KPI_new.sh"   
./CRH_export_cell_hour_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &   
#echo "export_imsi_day_kpi.sh"
#./export_imsi_day_kpi.sh    
echo "CRH_export_imsi_cell_hour_KPI_new.sh"   
./CRH_export_imsi_cell_hour_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &    
#echo "export_mr_cell_day_kpi.sh" 
#./export_mr_cell_day_kpi.sh 
echo "CRH_export_mr_cell_grid_hour_KPI_new.sh"
./CRH_export_mr_cell_grid_hour_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &  
#echo "export_mr_imsi_day_kpi.sh"
#./export_mr_imsi_day_kpi.sh 
echo "CRH_export_mr_cell_hour_kpi_new.sh"
./CRH_export_mr_cell_hour_kpi_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &
#./CRH_export_mr_imsi_cell_grid_hour_KPI_new.sh 
echo "CRH_export_mr_imsi_cell_grid_hour_KPI_new.sh"
./CRH_export_mr_imsi_cell_grid_hour_KPI_new.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} ${DEFAULTDB} &

wait  
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_user_ana_base60 volte_gt_user_ana_base60 13 4"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_user_ana_base60 13 2 
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_cell_ana_base60 volte_gt_cell_ana_base6 11 4"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/volte_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} volte_gt_cell_ana_base60 11 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_user_ana_base60 mr_gt_user_ana_base60 17 4"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_user_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_user_ana_base60 17 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_cell_ana_base60 mr_gt_cell_ana_base60 15 4"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_cell_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_cell_ana_base60 15 2
echo "./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_grid_ana_base60 mr_gt_grid_ana_base60 33 4"
./hdfs2db.sh hdfs://dtcluster/user/hive/warehouse/${DB}.db/mr_gt_grid_ana_base60/dt=${ANALY_DATE}/h=${ANALY_HOUR} mr_gt_grid_ana_base60 33 2
