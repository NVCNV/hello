#!/bin/sh
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3
INITDB=$4
#hdfs dfs -mkdir -p /user/hive/warehouse/${DB}.db/tb_xdr_ifc_gmmwmgmimjisc_new/dt=${ANALY_DATE}/h=${ANALY_HOUR}

hive<<EOF
USE ${DB};
alter table lte_mro_source_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table lte_mro_source_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/lte_mro_source/${ANALY_DATE}/${ANALY_HOUR}";

alter table ${INITDB}.lte_mro_source drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table ${INITDB}.lte_mro_source add partition(dt="$ANALY_DATE",h="$ANALY_HOUR")location "/datang2/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}";


alter table tb_xdr_ifc_uu_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_uu_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/tb_xdr_ifc_uu/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_x2_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_x2_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/tb_xdr_ifc_x2/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_sv_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_sv_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/sv_xdr/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_sgs_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_sgs_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/sgs/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1mme_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1mme_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/tb_xdr_ifc_s1mme/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_gxrx_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_gxrx_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/tb_xdr_ifc_gxrx/${ANALY_DATE}/${ANALY_HOUR}";


alter table tb_xdr_ifc_gmmwmgmimjisc_new drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_gmmwmgmimjisc_new add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") location "/datang2/output/xdrnew/tb_xdr_ifc_mw/${ANALY_DATE}/${ANALY_HOUR}";

alter table kpi_mid_cell_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table kpi_mid_cell_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

alter table kpi_mid_cell_day drop partition(dt="$ANALY_DATE");
alter table kpi_mid_cell_day add partition(dt="$ANALY_DATE");

alter table kpi_mid_imsi_cell_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table kpi_mid_imsi_cell_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

alter table kpi_mid_imsi_cell_day drop partition(dt="$ANALY_DATE");
alter table kpi_mid_imsi_cell_day add partition(dt="$ANALY_DATE");

alter table mro_kpi_mid_cell_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mro_kpi_mid_cell_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

alter table mro_kpi_mid_cell_day drop partition(dt="$ANALY_DATE");
alter table mro_kpi_mid_cell_day add partition(dt="$ANALY_DATE");

alter table mro_kpi_mid_cell_grid_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mro_kpi_mid_cell_grid_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

alter table mro_kpi_mid_cell_grid_day drop partition(dt="$ANALY_DATE");
alter table mro_kpi_mid_cell_grid_day add partition(dt="$ANALY_DATE");

alter table mro_kpi_mid_imsi_cell_grid_hour drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table mro_kpi_mid_imsi_cell_grid_hour add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

alter table mro_kpi_mid_imsi_cell_grid_day drop partition(dt="$ANALY_DATE");
alter table mro_kpi_mid_imsi_cell_grid_day add partition(dt="$ANALY_DATE");

alter table volte_gt_busi_user_data drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table volte_gt_busi_user_data add partition(dt="$ANALY_DATE",h="$ANALY_HOUR");

EOF
echo "hdfs dfs -cp /datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/tb* /user/hive/warehouse/${DB}.db/tb_xdr_ifc_gmmwmgmimjisc_new/dt=${ANALY_DATE}/h=${ANALY_HOUR}"
hdfs dfs -cp /datang2/output/xdrnew/tb_xdr_ifc_mw/${ANALY_DATE}/${ANALY_HOUR}/tb* /user/hive/warehouse/${DB}.db/tb_xdr_ifc_gmmwmgmimjisc_new/dt=${ANALY_DATE}/h=${ANALY_HOUR}/
hdfs dfs -cp /datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE* /user/hive/warehouse/shanxikpi2.db/volte_gt_busi_user_data/dt=${ANALY_DATE}/h=${ANALY_HOUR}/
