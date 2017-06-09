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
       |volte_users,
       |users
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
         |region,
         |city,
         |'$cal_date' ttime,
         |e.frequency,
         |count(distinct cellid) cell_num,
         |sum(gt_users) gtusers,
         |sum(users)-sum(gt_users) commusers,
         |(case when (count(distinct cellid)=0) then 0
         |else (cast (sum(gt_users)/count(distinct cellid) as int ) ) end) cellavguses
         | from
         |(
         |select
         |b.region,
         |b.city,
         |b.frequency,
         |a.cellid,
         |gt_users,
         |users
         |from gt_pulse_cell_base60 a
         |inner join ltecell b
         |on a.cellid=b.cellid
         |) e
         |group by e.city,e.region,e.frequency
       """.stripMargin).show()
//      .write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_freq_baseday/dt=$ANALY_DATE""")






  /*
  count(c.cellid) cell_num,

  sql(
      s"""
         |
         |select
         |d.region,
         |d.city,
         |'$cal_date' ttime,
         |d.frequency,
         |
         |b.gtusers,
         |e.cell_num cell_num,
         |round( b.cellavguses*100,0)
         |
         |from gt_pulse_detail_base60 a
         |
         |inner join (
         |  select
         |
         |  sum(c.gt_users) gtusers,
         |  round((sum(c.gt_users)/sum(c.users)),3) cellavguses,
         |
         |from gt_pulse_detail_base60 gt
         |inner join gt_pulse_cell_base60 c
         |on gt.cellid=c.cellid
         |group by gt.cellid
         |) b
         |on a.cellid=b.cellid
         |inner join ltecell d
         |  on a.cellid=d.cellid
         |inner join(
         |select count(cellid) cell_num,cellid
         |from ltecell group by frequency,cellid
         |) e
         |on a.cellid=e.cellid
         |
         |
       """.stripMargin).show()*/
  }





}
