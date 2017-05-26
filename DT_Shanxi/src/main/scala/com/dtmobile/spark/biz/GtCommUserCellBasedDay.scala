package com.dtmobile.spark.biz

import org.apache.spark.sql.SparkSession

/**
  * Created by zhoudehu on 2017/5/26/0026.
  */
class GtCommUserCellBasedDay(ANALY_DATE: String,DDB: String,warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

  def analy(implicit sparkSession: SparkSession): Unit ={
    gtCommUserCell
  }

  def gtCommUserCell(implicit sparkSession: SparkSession): Unit ={

    import  sparkSession.sql

    sql(
      s"""
         |
         |select
         |l.mcc,
         |l.city,
         |l.CELLID cellid,
         |l.CELLNAME cellname,
         |max(user2) maxusers
         |from
         |(select users,cellid,t2.user2 from
         |(select t.users,t.cellid,t.user2 from
         |        (select users,hours,cellid,sum(users)user2
         |         from gt_pulse_cell_min
         |          where dt="20170227" and sub_pulse_type=0
         |          group by hours,users,cellid
         |          having count(1)>=10 ) t
         |     group by t.users,t.cellid,t.user2
         |     having count(1)>=30)t2
         |group by cellid,users,user2
         |having count(1)>3 )t3
         | inner join ltecell l
         | on l.cellid=t3.cellid
         | group by l.cellid,l.city,l.cellname,l.mcc;
         |
         |
       """.stripMargin)



  }


}
