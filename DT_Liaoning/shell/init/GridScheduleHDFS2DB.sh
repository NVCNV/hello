#!/bin/bash

export HADOOP_CONF_DIR=/opt/app/hdconf

ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3
ORACLE_ADDR=$4
SCRIPT_ADDR=$5
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mrs_dlbestrow_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 1 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_joinuser_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 2 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mrs_overcover_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 3 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_disturb_pretreate60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 4 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mrs_dlbestrow_grid_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 5 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_overlap_grid_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 6 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/grid_ltemrkpi60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 7 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/cell_ltemrkpitemp/dt=${ANALY_DATE}/h=${ANALY_HOUR} 8 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_overlap_b_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 9 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/cell_ltenewmrkpi60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 10 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_disturb_sec/dt=${ANALY_DATE}/h=${ANALY_HOUR} 11 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_disturb_ana/dt=${ANALY_DATE}/h=${ANALY_HOUR} 12 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_disturb_mix/dt=${ANALY_DATE}/h=${ANALY_HOUR} 13 2

sh ${SCRIPT_ADDR}/GridanHDFS2db.sh ${ORACLE_ADDR} ${DIR}/lte_mro_adjcover_ana60/dt=${ANALY_DATE}/h=${ANALY_HOUR} 14 2
