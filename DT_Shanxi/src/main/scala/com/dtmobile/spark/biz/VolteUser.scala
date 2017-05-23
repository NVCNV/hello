package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/19/0019.
  */
class VolteUser(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"

  val currentHour = ANALY_HOUR.toInt
  var lastHour = ""
  if( 0<currentHour && currentHour<10 ){
    lastHour = "0"+currentHour.-(1)
  }else if(currentHour==0){
    lastHour = "00"
  }else{
    lastHour =currentHour.toString
  }
  val cal_date2 = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + lastHour + ":00:00"

  def analyse(implicit sparkSession: SparkSession): Unit = {
    init(sparkSession)
  }

  def init(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    sql(
      s"""alter table $DDB.VOLTE_USER_DATA add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/VOLTE_USER_DATA/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)

    //取出正常的数据，override
    sql(
      s"""select
         '$cal_date',
          t.imsi,
          from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'mm')  procedurestarttime,
          from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'mm') procedureendtime
          from $SDB.tb_xdr_ifc_mw t
          where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
         group by t.imsi,t.procedurestarttime,t.procedureendtime
        having from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'HH')=from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'HH')
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$ANALY_HOUR""")
    //取出上一个小时的数据，append到上一个小时
    sql(
      s"""select
         |'$cal_date2',
         |t.imsi,
         |from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'mm')  procedurestarttime,
         |'00' procedureendtime
         |from $SDB.tb_xdr_ifc_mw t
         |where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
         |group by t.imsi,t.procedurestarttime,procedureendtime
         | having from_unixtime(cast(round(procedurestarttime /1000) as bigint),'HH')=$lastHour
        """.stripMargin).repartition(2).write.mode(SaveMode.Append).csv(s"""$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$lastHour""")

         //取出当前小时的数据，append
    sql(
        s"""
           |select
           |'$cal_date',
           |t.imsi,
           |'00' procedurestarttime,
           |from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'mm')  procedureendtime
           |from $SDB.tb_xdr_ifc_mw t
           |where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
           |group by t.imsi,procedurestarttime,t.procedureendtime
           | having from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'HH')=$lastHour
           | and from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'HH')=$ANALY_HOUR
         """.stripMargin).repartition(2).write.mode(SaveMode.Append).csv(s"""$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$ANALY_HOUR""")



  }

}
