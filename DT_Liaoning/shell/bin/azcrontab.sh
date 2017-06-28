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
PROJECT=CRONTAB
FLOW=Crontab
KEYSTORE=/opt/app/azkaban/web/conf/keystore

java -jar $JAR $URL $USERNAME $PASSWD $PROJECT $FLOW $PASSWD $KEYSTORE $KEYSTORE