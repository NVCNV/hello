package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/25/0025.
  */
class GtUserFreqDay(ANALY_DATE: String,DDB: String,warhouseDir: String,ORCAL:String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

//  var oracle = "jdbc:oracle:thin:@"+ORCAL

  def analyse(implicit sparkSession: SparkSession): Unit ={
    gtUserFreqDay(sparkSession)
  }



  def gtUserFreqDay(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql
  sql(
    s"""
       |select
       |cellid,
       |gt_users,
       |volte_users
       |from $DDB.gt_pulse_cell_base60
       |where dt=$ANALY_DATE
     """.stripMargin).createOrReplaceTempView("gt_pulse_cell_base60")

    sql(
      s"""
         |select
         |cellid,
         |gtuser_flag,
         |volteuser_flag
         |from $DDB.gt_pulse_detail_base60
         |where dt=$ANALY_DATE
       """.stripMargin).createOrReplaceTempView("gt_pulse_detail_base60")

    sql(s"""alter table $DDB.gt_freq_baseday drop if  exists partition(dt="$ANALY_DATE")""".stripMargin)

    sql(s"""alter table $DDB.gt_freq_baseday add  partition(dt=$ANALY_DATE)""".stripMargin)

    sql(
      s"""
         |select
         |b.region line_name,
         |b.city,
         |'$cal_date' ttime,
         |gt.sfreq cell_feq,
         |count(c.cellid) cell_num,
         |sum(c.gt_users) gtusers,
         |sum(c.volte_users) commusers,
         |(sum(c.gt_users)/sum(c.volte_users)) cellavguses
         |from gt_pulse_detail_base60 a
         |inner join ltecell b
         |on a.cellid=b.cellid
         |inner join gt_pulse_cell_base60 c
         |on a.cellid=c.cellid
         |inner join 	$DDB.gt_balence_pair gt
         |on a.cellid=gt.scellid
         |group by b.cellid,gt.sfreq,b.region,b.city
         |
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_freq_baseday/dt=$ANALY_DATE""")

  }





}
