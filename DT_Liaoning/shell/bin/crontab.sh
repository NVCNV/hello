#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin
mypath="$(cd "$(dirname "$0")"; pwd)"
cd $mypath
JAR=/dt/lib/DT_Core-1.0-SNAPSHOT.jar
URL=https://fg03:8443
USERNAME=azkaban
PASSWD=azkaban
PROJECT=Analy
FLOW=JobComplete
KEYSTORE=/opt/app/azkaban/web/conf/keystore

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
#ANALY_DATE=20170503
#ANALY_HOUR=14
HOST=taiyue
HUSERNAME=datang
HPASSWD=Datang@12
PORT=50072
CMD_EXEC=" /cup/d4/datang/bin/hdfs2local.sh;ls /cup/d4/datang/TEMP/s1mme_orgn/$ANALY_DATE/$ANALY_HOUR"
java -jar $JAR $HOST $HUSERNAME $HPASSWD $PORT $CMD_EXEC
java -jar $JAR $URL $USERNAME $PASSWD $PROJECT $FLOW $PASSWD $KEYSTORE $KEYSTORE ANALY_DATE $ANALY_DATE ANALY_HOUR $ANALY_HOUR