package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/19/0019.
  */
class VolteUser(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String,sourceDir:String) {
  val cal_date:String = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"

  val currentHour:Int = ANALY_HOUR.toInt
  var lastHour = ""
  if( 0<currentHour && currentHour<10 ){
    lastHour = "0"+currentHour.-(1)
  }else if(currentHour==0){
    lastHour = "00"
  }else{
    lastHour =currentHour.-(1).toString
  }
  val cal_date2:String = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + lastHour + ":00:00"

  def analyse(implicit sparkSession: SparkSession): Unit = {
    init(sparkSession)
  }

  def init(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    sql(s"""alter table $DDB.volte_user_data drop if  exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")""")

    sql(
      s"""alter table $DDB.volte_user_data add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)
    sql(
      s"""alter table $SDB.TB_XDR_IFC_GMMWMGMIMJISC add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$sourceDir/TB_XDR_IFC_GMMWMGMIMJISC/$ANALY_DATE/$ANALY_HOUR'
       """.stripMargin)

    //取出正常的数据，override
   val volte_user1 = sql(
      s"""
         select t2.ttime,t2.hours,t2.imsi,t2.procedurestarttime1,t2.procedureendtime1 from (
         | select
         |   '$cal_date' ttime,
         |   '$ANALY_HOUR' hours,
         |    t.imsi,
         |    from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'mm')  procedurestarttime1,
         |    from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'mm') procedureendtime1,
         |    from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'HH')  procedurestarttime2,
         |    from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'HH') procedureendtime2
         |    from $SDB.TB_XDR_IFC_GMMWMGMIMJISC t
         |    where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
         |     group by t.imsi,procedurestarttime,procedureendtime
         |) t2
         |     where procedurestarttime2="$ANALY_HOUR" and procedureendtime2="$ANALY_HOUR"
         |     group by ttime,hours,imsi,procedurestarttime1,procedureendtime1
        """.stripMargin)

    //取出上一个小时的数据，append到上一个小时
    sql(
      s"""
         select t2.ttime,t2.hours,t2.imsi,t2.procedurestarttime1,t2.procedureendtime1 from (
          select
             '$cal_date2' ttime,
             '$lastHour' hours,
             t.imsi,
             from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'mm')  procedurestarttime1,
             '00' procedureendtime1,
             from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'HH')  procedurestarttime2,
             from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'HH') procedureendtime2
             from $SDB.TB_XDR_IFC_GMMWMGMIMJISC t
             where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
             group by t.imsi,t.procedurestarttime,procedureendtime
           ) t2
              where procedurestarttime2=$lastHour and procedureendtime2=$ANALY_HOUR
              group by ttime,hours,imsi,procedurestarttime1,procedureendtime1
        """.stripMargin).repartition(2).write.mode(SaveMode.Append).csv(s"""$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$lastHour""")


         //取出当前小时的数据，append
         val volte_user2 =  sql(
        s"""
            |select t2.ttime,t2.hours,t2.imsi,t2.procedurestarttime1,t2.procedureendtime1 from (
            |select
		        '$cal_date' ttime,
		        '$ANALY_HOUR' hours,
            t.imsi,
		        '00' procedurestarttime1,
		        from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'mm')  procedureendtime1,
		        from_unixtime(cast(round(t.procedurestarttime /1000) as bigint),'HH')  procedurestarttime2,
		        from_unixtime(cast(round(t.procedureendtime /1000) as bigint),'HH') procedureendtime2
		        from $SDB.TB_XDR_IFC_GMMWMGMIMJISC t
		        where dt="$ANALY_DATE" and h="$ANALY_HOUR" and t.Interface=14 and t.imsi is not null  and t.imsi!=''
		        group by t.imsi,procedurestarttime,t.procedureendtime
		     ) t2
           where procedurestarttime2=$lastHour and procedureendtime2=$ANALY_HOUR
           group by ttime,hours,imsi,procedurestarttime1,procedureendtime1
         """.stripMargin)


    volte_user1.union(volte_user2).createOrReplaceTempView("volte_user")

    sql(
      s"""
         |select ttime,hours,imsi,procedurestarttime1,procedureendtime1
         |from  volte_user
         |group by ttime,hours,imsi,procedurestarttime1,procedureendtime1
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/volte_user_data/dt=$ANALY_DATE/h=$ANALY_HOUR""")


  }

}
