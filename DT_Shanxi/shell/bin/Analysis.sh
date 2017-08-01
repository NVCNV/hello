#!/bin/bash
#此脚本如非自动调度,先执行其他ANALY_HOUR的数据,最后再执行ANALY_HOUR=03的数据
ANALY_DATE=$1
ANALY_HOUR=$2
#原始数据库
SOURCEDB=$3
#结果数据库
TARGETDB=$4
#sparkmaster ip 172.30.4.189
MASTER=$5
#spark程序 用到的oralce
OracleURL=$6
#原始数据路径
SOURCEDIR=$7
#版本,userkpi用到,1 为山西,0为辽宁
VERSINCONTROL=$8
#导出数据到的oracle
Oracle=$9

if  [ ${ANALY_HOUR} = 03 ];then
    echo "high spedd trail Day Analysis"
    ./highSpeed/repeat_analy_day.sh ${ANALY_DATE} ${TARGETDB} ${Oracle}

    echo "Business KPI Day Analysis"
    NEXT_ANALY_DATE=`date -d "${ANALY_DATE} +1 day" +%Y%m%d`
   ./busiKPI/BusKpi.sh ${NEXT_ANALY_DATE} ${ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${MASTER} ${OracleURL} ${SOURCEDIR} ${VERSINCONTROL}
   ./busiKPI/Kpi2db_day.sh ${ANALY_DATE} ${TARGETDB} ${Oracle} >> Kpi2db_day.log 2>&1

    echo "Volume Day Analysis"
    ./volume/VolumeRun.sh ${NEXT_ANALY_DATE} ${ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${MASTER} ${OracleURL} ${SOURCEDIR}
    ./volume/Volume2db_day.sh ${ANALY_DATE} ${TARGETDB} ${Oracle} >>Volume2db_day.log 2>&1
else
    echo "high spedd trail Hour Analysis"
    ./highSpeed/repeat_analy.sh ${ANALY_DATE} ${ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${SOURCEDIR} ${Oracle}

    echo "Business KPI Hour Analysis"
   ./busiKPI/BusKpi.sh ${ANALY_DATE} ${ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${MASTER} ${OracleURL} ${SOURCEDIR} ${VERSINCONTROL}
   ./busiKPI/Kpi2db_hour.sh ${ANALY_DATE} ${ANALY_HOUR} ${TARGETDB} ${Oracle} >>Kpi2db_hour.log 2>&1

    echo "Volume Hour Analysis"
    NEXT_ANALY_HOUR=`date -d "${ANALY_HOUR} +1 hour" +%H`
    #if [ ${NEXT_ANALY_HOUR} = 00 ]; then
    	#ANALY_DATE1=`date -d "${ANALY_DATE} +1 day" +%Y%m%d`
	   #	./volume/VolumeRun.sh ${ANALY_DATE1} ${NEXT_ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${MASTER} ${OracleURL} ${SOURCEDIR}
	   	#./volume/Volume2db_hour.sh ${ANALY_DATE} ${ANALY_HOUR} ${TARGETDB} ${Oracle} >> Volume2db_hour.log 2>&1
    #else
	   	./volume/VolumeRun.sh ${ANALY_DATE} ${NEXT_ANALY_HOUR} ${SOURCEDB} ${TARGETDB} ${MASTER} ${OracleURL} ${SOURCEDIR}
	   	./volume/Volume2db_hour.sh ${ANALY_DATE} ${ANALY_HOUR} ${TARGETDB} ${Oracle} >> Volume2db_hour.log 2>&1
    #fi
fi
exit 0
