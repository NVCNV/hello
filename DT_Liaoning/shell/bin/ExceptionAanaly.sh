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
EXCEPTION_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ExceptionCommonJob"

SOURCE_SVR="hdfs://dtcluster/datang"
LTE_MRO_SOURCE=${SOURCE_SVR}/LTE_MRO_SOURCE/${today}/${hour}/*
SV_TABLE=${SOURCE_SVR}/volte_sv/${today}/${hour}/*
EXCEPTION_MAP="/datang/exception_map/EXCEPTIONMAP.tsv"
cellMR=hdfs://dtcluster/user/hive/warehouse/dcl.db/cell_mr/dt=${today}/h=${hour}/000000_0
ltecell="/datang/ltcell/ltcell.csv"
T_PROCESS="/datang/t_process/t_process.csv"
EXCEPTION_OUT=hdfs://dtcluster/user/hive/warehouse/dcl.db/exception_analysis/dt=${today}/h=${hour}

${HADOOP} fs -rm -R ${EXCEPTION_OUT}
${HADOOP} jar ${JAR_FILE} ${EXCEPTION_MAIN} \
${ETYPE_OUT}/TB_XDR_IFC_UU/* \
${LTE_MRO_SOURCE}/* \
${ETYPE_OUT}/VOLTE_ORGN/* \
${ETYPE_OUT}/S1MME_ORGN/* \
${ETYPE_OUT}/TB_XDR_IFC_X2/* \
${ETYPE_OUT}/RX/* \
/liaoning/volte_sv.txt \
${EXCEPTION_OUT} \
${EXCEPTION_MAP} \
${cellMR} \
${ltecell}

