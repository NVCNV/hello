package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by zhangchao15 on 2017/5/26.
  */
class PulseDetailHour(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    pulseDetailHour(sparkSession)
  }

  def pulseDetailHour(sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table gt_pulse_cell_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
                LOCATION 'hdfs://dtcluster/$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR'
        """)
    sql(
      s"""
         |select ttime,
         |       hours,
         |       cellid,
         |       row_number() over(partition by cellid order by pluem),
         |       1,
         |       c1,
         |       m1,
         |       musers,
         |       mgtusers,
         |       mvlusers
         |  from (select ttime,
         |               hours,
         |               cellid,
         |               pluem,
         |               count(sub_pulse_mark) as c1,
         |               min(sub_pulse_mark) as m1,
         |               max(users) as musers,
         |               max(gt_users) as mgtusers,
         |               max(volte_users) as mvlusers
         |          from (select ttime,
         |                       hours,
         |                       cellid,
         |                       sub_pulse_mark,
         |                       sub_pulse_mark - row_number() over(partition by cellid order by sub_pulse_mark) as pluem,
         |                       users,
         |                       gt_users,
         |                       volte_users
         |                  from gt_pulse_cell_min
         |                 where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         |                 and sub_pulse_type = 1) a
         |         group by ttime, hours, cellid, pluem) b
       """.stripMargin).createOrReplaceTempView("gt_pulse_cell_base60_tmp")
    sql(
      s"""
         |select pct.ttime,
         |       pct.hours,
         |       pct.cellid,
         |       pct.pulse_mark,
         |       1,
         |       count(gpc.sub_pulse_mark) ,
         |       min(pct.first_pulse_mark),
         |       max(pct.users) ,
         |       max(pct.gt_users) ,
         |       max(pct.volte_users),
         |       count(distinct gpd.imsi),
         |       count(distinct case when gpd.gtuser_flag = 1 then 1 else 0 end),
         |       count(distinct case when gpd.volteuser_flag =1 then 1 else 0 end)
         |  from gt_pulse_cell_base60_tmp pct
         | inner join gt_pulse_detail gpd
         |    on pct.cellid =gpd.cellid
         | inner join gt_pulse_cell_min gpc
         |    on gpd.sub_pulse_mark = gpc.sub_pulse_mark
         |    where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         | group by pct.ttime,pct.hours,pct.cellid, pct.pulse_mark

        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
