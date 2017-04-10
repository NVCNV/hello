#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin


JAR=/dt/lib/DT_Core-1.0-SNAPSHOT.jar
URL=https://fg04:8443
USERNAME=azkaban
PASSWD=azkaban
PROJECT=Analy
FLOW=JobAnaly
KEYSTORE=/opt/app/azkaban/web/conf/keystore

ANALY_DATE=$1
ANALY_HOUR=$2

java -jar $JAR $URL $USERNAME $PASSWD $PROJECT $FLOW $PASSWD $KEYSTORE $KEYSTORE $ANALY_DATE $ANALY_HOUR