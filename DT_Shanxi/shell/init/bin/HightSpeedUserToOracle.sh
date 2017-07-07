#!/bin/bash
TAKING_DATE=$1
TAKING_HOUR=$2
DAPATH=$3
HDFS_ADDR=" hdfs://dtcluster/datang2/output"

DB_ADDR="userid=scott/tiger@${DAPATH}"



NEW_TABLES="tb_xdr_ifc_mw tb_xdr_ifc_gxrx tb_xdr_ifc_x2 tb_xdr_ifc_sv lte_mro_source tb_xdr_ifc_uu tb_xdr_ifc_s1mme volte_gtuser_data tb_xdr_ifc_sgs"



LOCALDIR="/dt/NewData"
CTLDIR="/dt/ctl"


rm -rf ${LOCALDIR}/$TAKING_DATE
#mkdir ${LOCALDIR}/$TAKING_DATE

echo "--------------------HighSpeedUser Data To Oracle.......----------------------"


for tableName in ${NEW_TABLES}
do
echo "------->get from ${HDFS_ADDR}/xdrnew/${tableName}/${TAKING_DATE}/${TAKING_HOUR}/* to ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}"
hdfs dfs -getmerge ${HDFS_ADDR}/xdrnew/${tableName}/${TAKING_DATE}/${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName}.dat
mkdir -p /dt/sqlldrLog/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/${tableName}.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/${tableName} log=/dt/sqlldrLog/${tableName}/${TAKING_DATE}/${TAKING_HOUR}
done

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/trainiden/${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/trainiden"
hdfs dfs -getmerge ${HDFS_ADDR}/trainiden/${TAKING_DATE}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/trainiden.dat
mkdir -p /dt/sqlldrLog/trainiden/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/trainiden.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/trainiden log=/dt/sqlldrLog/trainiden/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/u1/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u1"
hdfs dfs -getmerge ${HDFS_ADDR}/u1/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u1.dat
mkdir -p /dt/sqlldrLog/u1/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/u1.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u1 log=/dt/sqlldrLog/u1/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/u2/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u2"
hdfs dfs -getmerge ${HDFS_ADDR}/u2/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u2.dat
mkdir -p /dt/sqlldrLog/u2/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/u2.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u2 log=/dt/sqlldrLog/u2/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/u3/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u3"
hdfs dfs -getmerge ${HDFS_ADDR}/u3/${TAKING_DATE}/${TAKING_HOUR} ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u3.dat
mkdir -p /dt/sqlldrLog/u3/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/u3.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/u3 log=/dt/sqlldrLog/u3/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/business/${TAKING_DATE}/${TAKING_HOUR}/loc_guser_mark* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/loc_guser_mark"
hdfs dfs -getmerge ${HDFS_ADDR}/business/${TAKING_DATE}/${TAKING_HOUR}/loc_guser_mark* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/loc_guser_mark.dat
mkdir -p /dt/sqlldrLog/loc_guser_mark/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/loc_guser_mark.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/loc_guser_mark log=/dt/sqlldrLog/loc_guser_mark/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/business/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_BUSI_USER* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_BUSI_USER"
hdfs dfs -getmerge ${HDFS_ADDR}/business/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_BUSI_USER* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_BUSI_USER.dat
mkdir -p /dt/sqlldrLog/VOLTE_GT_BUSI_USER/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/VOLTE_GT_BUSI_USER.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_BUSI_USER log=/dt/sqlldrLog/VOLTE_GT_BUSI_USER/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/free/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_FREE_USER* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_FREE_USER"
hdfs dfs -getmerge ${HDFS_ADDR}/free/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_FREE_USER* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_FREE_USER.dat
mkdir -p dt/sqlldrLog/VOLTE_GT_FREE_USER/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/VOLTE_GT_FREE_USER.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/VOLTE_GT_FREE_USER log=/dt/sqlldrLog/VOLTE_GT_FREE_USER/${TAKING_DATE}/${TAKING_HOUR}

echo "------->hdfs dfs -getmerge ${HDFS_ADDR}/cellMR/${TAKING_DATE}/${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/cellMR"
hdfs dfs -getmerge ${HDFS_ADDR}/cellMR/${TAKING_DATE}/${TAKING_HOUR}/* ${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/cellMR.dat
mkdir -p /dt/sqlldrLog/cellMR/${TAKING_DATE}/${TAKING_HOUR}
sqlldr ${DB_ADDR} control=${CTLDIR}/cellMR.ctl data=${LOCALDIR}/${TAKING_DATE}/${TAKING_HOUR}/cellMR log=/dt/sqlldrLog/cellMR/${TAKING_DATE}/${TAKING_HOUR}


echo "----------------------------HighSpeedUser Data To Oracle Successful.....----------------------"

exit 0
