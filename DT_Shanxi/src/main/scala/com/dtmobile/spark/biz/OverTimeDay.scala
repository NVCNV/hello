package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2017/5/27.
  */
class OverTimeDay(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    overTimeDay(sparkSession)
    }

    def overTimeDay(sparkSession: SparkSession): Unit = {
      import sparkSession.sql
      sql(s"use $DDB")
      sql(
        s"""alter table gt_overtimelen_baseday add if not exists partition(dt=$ANALY_DATE)
                LOCATION 'hdfs://dtcluster/$warhouseDir/gt_overtimelen_baseday/dt=$ANALY_DATE
        """)

      val t1 = sql("select over_pulse_times from gt_capacity_config ").collectAsList()
      var overPlseTimes = 4
      if (t1.size() > 0) {
        overPlseTimes = t1.get(0).getInt(0)
      }
      var overPulseTimelen = 4
      val t2 = sql("select over_pulse_timelen from gt_capacity_config ").collectAsList()
      if (t2.size() > 0) {
        overPulseTimelen = t2.get(0).getInt(0)
      }
      val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"
      sql(
        s"""
           |select
           |       ltcel.line_name,
           |       ltcel.city,
           |       ${cal_date},
           |       ttt.cellid,
           |       ltcel.cellname,
           |       ttt.min_hour,
           |       ttt.min_pluse_timelen,
           |       ttt.max_hour,
           |       ttt.max_pluse_timelen
           |  from (SELECT cellid,
           |               max(min_hour) min_hour,
           |               max(min_pluse_timelen) min_pluse_timelen,
           |               max(max_hour) max_hour,
           |               max(max_pluse_timelen) max_pluse_timelen
           |          FROM (SELECT a.cellid,
           |                       0 min_hour,
           |                       0 min_pluse_timelen,
           |                       min(b. HOURS) max_hour,
           |                       min(a.pluse_timelen_max) max_pluse_timelen
           |                  FROM (SELECT cellid, max(pulse_timelen) pluse_timelen_max
           |                          FROM (SELECT cellid,
           |                                       HOURS,
           |                                       max(pulse_timelen) pulse_timelen,
           |                                       sum(CASE
           |                                             WHEN pulse_timelen > ${overPulseTimelen} THEN
           |                                              1
           |                                             ELSE
           |                                              0
           |                                           END) times
           |                                  FROM gt_pulse_cell_base60
           |                                  where dt="$ANALY_DATE" and h="$ANALY_HOUR"
           |                                 GROUP BY cellid, HOURS) t1
           |                         WHERE t1.times > ${overPlseTimes}
           |                         GROUP BY cellid) a
           |                 INNER JOIN gt_pulse_cell_base60 b
           |                    ON a.pluse_timelen_max = b.pulse_timelen
           |                   AND a.cellid = b.cellid
           |                 GROUP BY a.cellid
           |                UNION ALL
           |                SELECT a.cellid,
           |                       min(b. HOURS) min_hour,
           |                       min(a.pluse_timelen_min) min_pluse_timelen,
           |                       0 max_hour,
           |                       0 max_pluse_timelen
           |                  FROM (SELECT cellid, min(pulse_timelen) pluse_timelen_min
           |                          FROM (SELECT cellid,
           |                                       HOURS,
           |                                        min(pulse_timelen) pulse_timelen,
           |                                       sum(CASE
           |                                             WHEN pulse_timelen > ${overPulseTimelen} THEN
           |                                              1
           |                                             ELSE
           |                                              0
           |                                           END) times
           |                                  FROM gt_pulse_cell_base60
           |                                  where dt="$ANALY_DATE" and h="$ANALY_HOUR"
           |                                GROUP BY cellid, HOURS) t1
           |                         WHERE t1.times > ${overPlseTimes}
           |                         GROUP BY cellid) a
           |                 INNER JOIN gt_pulse_cell_base60 b
           |                    ON a.pluse_timelen_min = b.pulse_timelen
           |                   AND a.cellid = b.cellid
           |                 GROUP BY a.cellid) tt
           |         GROUP BY tt.cellid) ttt
           |  left join gt_publicandprofess_new_cell ltcel
           |    on ttt.cellid = ltcel.cellid

        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_overtimelen_baseday/dt=$ANALY_DATE")
  }
}
