#!/bin/bash
export JAVA_HOME=/opt/app/java
export KYLIN_HOME=/opt/app/kylin
export MAVEN_HOME=/opt/app/maven
export ANT_HOME=/opt/app/ant
export SCALA_HOME=/opt/app/scala
export HADOOP_HOME=/opt/app/hadoop
export HIVE_HOME=/opt/app/hive
export HADOOP_CONF_DIR=/opt/app/hdconf
export HBASE_CONF_DIR=/opt/app/hbconf
export HBASE_HOME=/opt/app/hbase
export SPARK_HOME=/opt/app/spark
export PATH=$PATH:$JAVA_HOME/bin:$SCALA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$MAVEN_HOME/bin:$HBASE_HOME/bin:$HBASE_HOME/sbin:$HIVE_HOME/bin:$KYLIN_HOME/bin:$ANT_HOME/bin

mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
ANALY_DATE=$1


./repeat_same_and_updown_checi.sh ${ANALY_DATE}  
./bushu/kpiAnalyday.sh ${ANALY_DATE} result  >> kpiAnaly_day.log 2>&1

#日期 Oracle数据库
sh HightSpeedUserToOracle_day.sh ${ANALY_DATE} hadoop >> Speed_day_toORacle.log