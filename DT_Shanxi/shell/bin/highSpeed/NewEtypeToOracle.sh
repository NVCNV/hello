#!/bin/bash

TAKING_DATE=$1
TAKING_HOUR=$2
ORACLEDB=$3
RESULTDB=$4
HDFS_ADDR=" hdfs://dtcluster/datang2/ETYPE_OUT/${TAKING_DATE}/${TAKING_HOUR}"

DB_ADDR="userid=scott/tiger@${ORACLEDB}"

LOCALDIR="/dt/tmpdata"
CTLDIR="/dt/ctl"

echo "--------hdfs dfs -getmerge ${HDFS_ADDR}/S1MME_ORGN* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_s1mme.dat--------"
hdfs dfs -getmerge ${HDFS_ADDR}/S1MME_ORGN* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_s1mme.dat
mkdir -p /dt/sqlldrLog/tb_xdr_ifc_s1mme/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/tb_xdr_ifc_s1mme.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_s1mme.dat log=/dt/sqlldrLog/tb_xdr_ifc_s1mme/${TAKING_DATE}/${TAKING_HOUR}

echo "--------hdfs dfs -getmerge ${HDFS_ADDR}/VOLTE_ORGN* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_mw.dat--------"
hdfs dfs -getmerge ${HDFS_ADDR}/VOLTE_ORGN* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_mw.dat
mkdir -p /dt/sqlldrLog/tb_xdr_ifc_mw/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/tb_xdr_ifc_mw.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_mw.dat log=/dt/sqlldrLog/tb_xdr_ifc_mw/${TAKING_DATE}/${TAKING_HOUR}

echo "--------hdfs dfs -getmerge ${HDFS_ADDR}/rx* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_gxrx.dat--------"
hdfs dfs -getmerge ${HDFS_ADDR}/rx* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_gxrx.dat
mkdir -p /dt/sqlldrLog/tb_xdr_ifc_gxrx/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/tb_xdr_ifc_gxrx.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_gxrx.dat log=/dt/sqlldrLog/tb_xdr_ifc_gxrx/${TAKING_DATE}/${TAKING_HOUR}

echo "--------hdfs dfs -getmerge ${HDFS_ADDR}/TB_XDR_IFC_UU* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_uu.dat--------"
hdfs dfs -getmerge ${HDFS_ADDR}/TB_XDR_IFC_UU* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_uu.dat
mkdir -p /dt/sqlldrLog/tb_xdr_ifc_uu/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/tb_xdr_ifc_uu.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_uu.dat log=/dt/sqlldrLog/tb_xdr_ifc_uu/${TAKING_DATE}/${TAKING_HOUR}

echo "--------hdfs dfs -getmerge ${HDFS_ADDR}/TB_XDR_IFC_X2* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_x2.dat--------"
hdfs dfs -getmerge ${HDFS_ADDR}/TB_XDR_IFC_X2* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_x2.dat
mkdir -p /dt/sqlldrLog/tb_xdr_ifc_x2/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/tb_xdr_ifc_x2.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/tb_xdr_ifc_x2.dat log=/dt/sqlldrLog/tb_xdr_ifc_x2/${TAKING_DATE}/${TAKING_HOUR}

echo "----------hdfs dfs -getmerge /user/hive/warehouse/${RESULTDB}.db/lte_cellmr_source/${TAKING_DATE}/${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/lte_cellmr_source.dat-----------"
hdfs dfs -getmerge /user/hive/warehouse/${RESULTDB}.db/lte_cellmr_source/dt=${TAKING_DATE}/h=${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/lte_cellmr_source.dat
mkdir -p /dt/sqlldrLog/lte_cellmr_source/${TAKING_DATE}
sqlldr ${DB_ADDR} control=${CTLDIR}/lte_cellmr_source.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/lte_cellmr_source.dat log=/dt/sqlldrLog/lte_cellmr_source/${TAKING_DATE}/${TAKING_HOUR}

exit 0























