package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/25/0025.
  */
class HighAttachCellDay(ANALY_DATE: String,DDB: String,warhouseDir: String,ORCAL:String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

  var oracle = "jdbc:oracle:thin:@"+ORCAL
  def analyse(implicit sparkSession: SparkSession): Unit ={
    highAttachCellDay(sparkSession)
  }

  def highAttachCellDay(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    var highattach_users = 300
    var highattach_times = 4

    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","(SELECT distinct CELL_ID, LINE_NAME,CELL_NAME,CITY FROM t_profess_net_cell) T")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("t_profess_net_cell")

    sql(s"""alter table $DDB.gt_highattach_baseday drop if  exists partition(dt="$ANALY_DATE")""".stripMargin)
    sql(s"""alter table $DDB.gt_highattach_baseday add  partition(dt=$ANALY_DATE)""".stripMargin)


    val t= sql("""select highattach_users,highattach_times from gt_capacity_config""").collectAsList()

    if(t.size()>0){
     highattach_users =  t.get(0).getAs("highattach_users")
     highattach_times = t.get(0).getAs("highattach_times")
    }

sql(
  s"""
     |select
     |T2.LINE_NAME,
     |t2.city,
     |'$cal_date' ttime,
     |t2.CELL_ID cellid,
     |t2.CELL_NAME cellname,
     |users2 as maxusers from
     |(select cellid,max(users1) users2 from
     |  (
     |select cellid,hours,max(sub_users_peak) users1
     |  from $DDB.gt_pulse_cell_base60
     |  where dt="$ANALY_DATE" group by cellid,hours
     |  ) t
     |  where t.users1>$highattach_users
     | group by cellid
     |   having count(1)>=$highattach_times
     |  ) t1
     |  inner join t_profess_net_cell t2 on t2.CELL_ID=t1.cellid
  """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_highattach_baseday/dt=$ANALY_DATE""")


  }




}
