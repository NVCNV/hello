#!/bin/bash
export HADOOP_CONF_DIR=/opt/app/hdconf
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1
DB=$2
ORACLE_NAME=$3

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

# 日期 hive结果数据库 oracle数据库
sh kpi_sqlldr2db_day.sh ${ANALY_DATE} ${DB} ${ORACLE_NAME}  >> kpi_sqlldr2db_day.log

