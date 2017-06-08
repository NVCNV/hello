#!/bin/bash

export HADOOP_CONF_DIR=/opt/app/hdconf

ANALY_DATE=$1
ANALY_HOUR=$2
DATABASE=$3
DIR=hdfs://dtcluster/user/hive/warehouse/${DATABASE}.db


sh GridanHDFS2db.sh ${DIR}/lte_mro_source_ana_tmp/${ANALY_DATE}/${ANALY_HOUR} 1 2

sh GridanHDFS2db.sh ${DIR}/lte_mro_disturb_pretreate60/${ANALY_DATE}/${ANALY_HOUR} 2 2

sh GridanHDFS2db.sh ${DIR}/ltecell/${ANALY_DATE}/${ANALY_HOUR} 3 2

sh GridanHDFS2db.sh ${DIR}/lte_mrs_dlbestrow_ana60/${ANALY_DATE}/${ANALY_HOUR} 4 2

sh GridanHDFS2db.sh ${DIR}/MR_INDOORANA_TEMP/${ANALY_DATE}/${ANALY_HOUR} 5 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_JOINUSER_ANA60/${ANALY_DATE}/${ANALY_HOUR} 6 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRS_OVERCOVER_ANA60/${ANALY_DATE}/${ANALY_HOUR} 7 2

sh GridanHDFS2db.sh ${DIR}/LTE2LTEADJ_PCI/${ANALY_DATE}/${ANALY_HOUR} 8 2

sh GridanHDFS2db.sh ${DIR}/LTE2LTEADJ/${ANALY_DATE}/${ANALY_HOUR} 9 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_DISTURB_PRETREATE60tmp/${ANALY_DATE}/${ANALY_HOUR} 10 2

sh GridanHDFS2db.sh ${DIR}/lte2lteadj_pci/${ANALY_DATE}/${ANALY_HOUR} 11 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_DISTURB_PRETREATE60/${ANALY_DATE}/${ANALY_HOUR} 12 2

sh GridanHDFS2db.sh ${DIR}/grid_view/${ANALY_DATE}/${ANALY_HOUR} 13 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRS_DLBESTROW_GRID_ANA60/${ANALY_DATE}/${ANALY_HOUR} 14 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_OVERLAP_GRID_ANA60/${ANALY_DATE}/${ANALY_HOUR} 15 2

sh GridanHDFS2db.sh ${DIR}/GRID_LTEMRKPI60/${ANALY_DATE}/${ANALY_HOUR} 16 2

sh GridanHDFS2db.sh ${DIR}/CELL_LTEMRKPITEMP/${ANALY_DATE}/${ANALY_HOUR} 17 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_OVERLAP_B_ANA60/${ANALY_DATE}/${ANALY_HOUR} 18 2

sh GridanHDFS2db.sh ${DIR}/CELL_LTENEWMRKPI60/${ANALY_DATE}/${ANALY_HOUR} 19 2

sh GridanHDFS2db.sh ${DIR}/CELL_LTEMRKPI60/${ANALY_DATE}/${ANALY_HOUR} 20 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_DISTURB_SEC/${ANALY_DATE}/${ANALY_HOUR} 21 2

sh GridanHDFS2db.sh ${DIR}/lte_mro_disturb_ana/${ANALY_DATE}/${ANALY_HOUR} 22 2

sh GridanHDFS2db.sh ${DIR}/lte_mro_disturb_mix/${ANALY_DATE}/${ANALY_HOUR} 23 2

sh GridanHDFS2db.sh ${DIR}/lte_mro_adjcover_ana60/${ANALY_DATE}/${ANALY_HOUR} 24 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_SOURCE_TMP/${ANALY_DATE}/${ANALY_HOUR} 25 2

sh GridanHDFS2db.sh ${DIR}/LTE_MRO_DISTURB_SEC/${ANALY_DATE}/${ANALY_HOUR} 26 2

sh GridanHDFS2db.sh ${DIR}/lte_mro_disturb_mix/${ANALY_DATE}/${ANALY_HOUR} 27 2