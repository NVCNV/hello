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


#日期 时间 hive结果数据库  Oracle数据库
sh kpi_sqlldr2db.sh ${ANALY_DATE} ${ANALY_HOUR} ${DB} hadoop   >> kpi_hour_toOracle.log