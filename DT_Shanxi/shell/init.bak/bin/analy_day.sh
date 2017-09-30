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
ANALY_DATE=`date +%Y%m%d`
ANALY_HOUR="`date -d ' -1 hour' +%H`"
CUR_DATE=`date +%Y%m%d`
CUR_HOUR="`date -d ' -0 hour' +%H`"
ANALY_DATE=$CUR_DATE
if [ $CUR_HOUR = 00 ]
then
   ANALY_DATE="`date -d ' -1 day' +%Y%m%d`"
else
   ANALY_DATE=$CUR_DATE
fi

echo $ANALY_DATE
echo $ANALY_HOUR

./bushu/kpiAnalyday.sh ${ANALY_DATE}  shanxikpi ddl >> kpiAnaly_day.log 2>&1
