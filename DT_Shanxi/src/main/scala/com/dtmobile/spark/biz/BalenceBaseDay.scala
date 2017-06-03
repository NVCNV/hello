package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by weiyaqin on 2017/5/27/0026.
  */
class BalenceBaseDay(ANALY_DATE: String,DDB: String,warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

  def analy(implicit sparkSession: SparkSession): Unit ={
    import  sparkSession.sql

    var balence_userrate:BigDecimal=0.5
    var balence_times = 10
    val t= sql("""select balence_userrate_f,balence_times from gt_capacity_config""").collectAsList()
    if(t.size()>0){
      balence_userrate =  t.get(0).getDecimal(0)
      balence_times = t.get(0).getAs("balence_times")
    }

    sql(s"use $DDB")
    sql(s"""alter table gt_balence_baseday drop if exists partition(dt=$ANALY_DATE)""")
    sql(s"""alter table gt_balence_baseday add if not exists partition(dt=$ANALY_DATE)
    LOCATION 'hdfs://dtcluster/$warhouseDir/gt_balence_baseday/dt=$ANALY_DATE'""")
    sql(
      s"""
         | select c.line,gt.city,c.ttime,c.cellid,c.cellname,c.pairname f1_f2,avgratio balenusesrateavg,c.avgcount balenusersavg from
         | (
         | select ttime,line,city,cellname,cellid,pairname,avg(balratio) avgratio,avg(balusers) avgcount, sum(bal) sumbal from
         | (
         | select a.*, (case when balratio<=$balence_userrate then 1 else 0 end) bal from gt_pulse_load_balence60 a
         | where dt="$ANALY_DATE"
         | ) b group by ttime,line,city,cellname,cellid,pairname
         | ) c
         |inner join gt_balence_pair gt
         |on gt.scellid=c.cellid
         |where sumbal >= 0
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_balence_baseday/dt=$ANALY_DATE")
  }
}
