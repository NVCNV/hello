ANALY_DATE=$1
ANALY_HOUR=$2
SDB=$3
DDB=$4
MASTER=$5
OracleUrl=$6
SOURCEDIR=$7
#MASTER=spark://172.30.4.189:7077



MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/bin/DT_Shanxi-1.0-SNAPSHOT.jar


/opt/app/spark/bin/spark-submit  \
 --class $MAIN_CLASS \
 --executor-memory 4G \
 --executor-cores 2 \
 $JAR $ANALY_DATE $ANALY_HOUR $SDB $DDB $MASTER $OracleUrl $SOURCEDIR 