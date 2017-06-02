package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2017/5/25.
  */
class SubPulseStatis(ANALY_DATE: String, ANALY_HOUR: String,  DDB: String, warhouseDir: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    subPulseStatis(sparkSession)
  }

  def subPulseStatis(sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table gt_pulse_cell_min add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           LOCATION 'hdfs://dtcluster/$warhouseDir/gt_pulse_cell_min/dt=$ANALY_DATE/h=$ANALY_HOUR'
      """)
    val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " "

    val t1 = sql("select sub_pulse_limit from gt_capacity_config ").collectAsList()
    var limit = 10
    if (t1.size() > 0) {
      limit = t1.get(0).getInt(0)
    }
    sql(
      s"""
         |select concat('${cal_date}',' ','t.hours,':',t.minutes,':','00'),
         |       t.hours,
         |       t.minutes,
         |       t.cellid,
         |       t.sub_pulse_mark,
         |       case
         |         when t.cgtuser > ${limit} then
         |          1
         |         else
         |          0
         |       end,
         |       cimsi,
         |       cgtuser,
         |       cimsi - cgtuser,
         |       cvolteuser
         |  from (select
         |               hours,
         |               minutes,
         |               cellid,
         |               sub_pulse_mark,
         |               count(case
         |                       when gtuser_flag = 1 then
         |                        1
         |                       else
         |                        null
         |                     end) cgtuser,
         |               count(imsi) cimsi,
         |               count(case
         |                       when volteuser_flag = 1 then
         |                        1
         |                       else
         |                        null
         |                     end) cvolteuser
         |          from gt_pulse_detail
         |          where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         |         group by  cellid,minutes,sub_pulse_mark,hours) t
         |
      """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_cell_min/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

}
