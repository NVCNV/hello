#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin

ANALY_DATE=$1
ANALY_HOUR=$2

#ANALY_DATE=`date +%Y%m%d`
#ANALY_HOUR="`date -d ' -0 hour' +%H`"

SOURCE_SVR="hdfs://dtcluster/liaoning"
JAR_FILE="/dt/lib/DT_mobile.jar"
ETYPE_MAIN="cn.com.dtmobile.hadoop.biz.exception.job.ProcessJob"

VOLTE_RX=${SOURCE_SVR}/volte_rx/${ANALY_DATE}/${ANALY_HOUR}/*
VOLTE_ORGN=${SOURCE_SVR}/volte_orgn/${ANALY_DATE}/${ANALY_HOUR}/*
S1MME_ORGN=${SOURCE_SVR}/s1mme_orgn/${ANALY_DATE}/${ANALY_HOUR}/*
TB_XDR_IFC_UU=${SOURCE_SVR}/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}/*
TB_XDR_IFC_X2=${SOURCE_SVR}/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}/*
ETYPE_OUT=${SOURCE_SVR}/ETYPE_OUT



hdfs dfs -rm -R -skipTrash ${ETYPE_OUT}

hadoop jar ${JAR_FILE} ${ETYPE_MAIN}  ${VOLTE_RX} ${VOLTE_ORGN} ${S1MME_ORGN} ${TB_XDR_IFC_UU} ${TB_XDR_IFC_X2} ${ETYPE_OUT}


