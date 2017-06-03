package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2017/5/27.
  */
class ShortTimeDay (ANALY_DATE: String, DDB: String, warhouseDir: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    shortTimeDay(sparkSession)
  }

  def shortTimeDay(sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table gt_shorttimelen_baseday add if not exists partition(dt=$ANALY_DATE)
                LOCATION 'hdfs://dtcluster/$warhouseDir/gt_shorttimelen_baseday/dt=$ANALY_DATE'
        """)

    val t1 = sql("select short_pulse_times from gt_capacity_config ").collectAsList()
    var shortPlseTimes = 4
    if (t1.size() > 0) {
      shortPlseTimes = t1.get(0).getInt(0)
    }
    var shortPulseTimeLen:BigDecimal = 4
    val t2 = sql("select short_pulse_timelen from gt_capacity_config ").collectAsList()
    if (t2.size() > 0) {
      shortPulseTimeLen = t2.get(0).getDecimal(0)
    }
    val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0, 2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"
    sql(
      s"""
         |select
         |       ltcel.line_name,
         |       ltcel.city,
         |       '${cal_date}' as ttime,
         |       ttt.cellid,
         |       ltcel.cell_name as cellname,
         |       ttt.min_pluse_timelen as minpluse_timelen,
         |       ttt.min_hour as minhour,
         |        ttt.max_pluse_timelen as maxpluse_timelen,
         |       ttt.max_hour as maxhour
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
         |                                             WHEN pulse_timelen <= ${shortPulseTimeLen} THEN
         |                                              1
         |                                             ELSE
         |                                              0
         |                                           END) times
         |                                  FROM ${DDB}.gt_pulse_cell_base60
         |                                  where dt="$ANALY_DATE"
         |                                 GROUP BY cellid, HOURS) t1
         |                         WHERE t1.times > ${shortPlseTimes}
         |                         GROUP BY cellid) a
         |                 INNER JOIN ${DDB}.gt_pulse_cell_base60 b
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
         |                                             WHEN pulse_timelen <= ${shortPulseTimeLen} THEN
         |                                              1
         |                                             ELSE
         |                                              0
         |                                           END) times
         |                                  FROM ${DDB}.gt_pulse_cell_base60
         |                                  where dt="$ANALY_DATE"
         |                                GROUP BY cellid, HOURS) t1
         |                         WHERE t1.times > ${shortPlseTimes}
         |                         GROUP BY cellid) a
         |                 INNER JOIN ${DDB}.gt_pulse_cell_base60 b
         |                    ON a.pluse_timelen_min = b.pulse_timelen
         |                   AND a.cellid = b.cellid
         |                 GROUP BY a.cellid) tt
         |         GROUP BY tt.cellid) ttt
         |  left join gt_publicandprofess_new_cell ltcel
         |    on ttt.cellid = ltcel.cell_id

        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_shorttimelen_baseday/dt=$ANALY_DATE")
  }
}
