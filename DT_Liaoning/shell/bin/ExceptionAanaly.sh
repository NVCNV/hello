#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath

ANALY_DATE=$1
ANALY_HOUR=$2

HADOOP=`which hadoop`
EXCEPTION_MAIN="cn.com.dtmobile.hadoop.biz.ExceptionCommonJob"

SOURCE_SVR="hdfs://dtcluster/liaoning"
LTE_MRO_SOURCE=${SOURCE_SVR}/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}/*
SV_TABLE=${SOURCE_SVR}/volte_sv/${ANALY_DATE}/${ANALY_HOUR}/*
EXCEPTION_MAP="${SOURCE_SVR}/exception_map/EXCEPTIONMAP.tsv"
cellMR=hdfs://dtcluster/user/hive/warehouse/dcl.db/cell_mr/dt=${ANALY_DATE}/h=${ANALY_HOUR}/*
ltecell="${SOURCE_SVR}/ltcell/ltcell.csv"
T_PROCESS="${SOURCE_SVR}/t_process/t_process.csv"
EXCEPTION_OUT=hdfs://dtcluster/user/hive/warehouse/dcl.db/exception_analysis/dt=${ANALY_DATE}/h=${ANALY_HOUR}

${HADOOP} fs -rm -R ${EXCEPTION_OUT}
${HADOOP} jar ${JAR_FILE} ${EXCEPTION_MAIN} \
${ETYPE_OUT}/TB_XDR_IFC_UU/* \
${LTE_MRO_SOURCE}/* \
${ETYPE_OUT}/VOLTE_ORGN/* \
${ETYPE_OUT}/S1MME_ORGN/* \
${ETYPE_OUT}/TB_XDR_IFC_X2/* \
${ETYPE_OUT}/RX/* \
${SOURCE_SVR}/volte_sv.txt \
${EXCEPTION_OUT} \
${EXCEPTION_MAP} \
${cellMR} \
${ltecell}

