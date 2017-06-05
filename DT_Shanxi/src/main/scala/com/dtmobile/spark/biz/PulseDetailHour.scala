package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by zhangchao15 on 2017/5/26.
  */
class PulseDetailHour(ANALY_DATE: String, ANALY_HOUR: String, DDB: String, warhouseDir: String) {
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
         |select ttime as ttime,
         |       hours as hours,
         |       cellid as cellid,
         |       row_number() over(partition by cellid order by pluem) as pulse_mark,
         |       1 as pulse_type,
         |       c1 as pulse_timelen,
         |       m1 as first_pulse_mark,
         |       scu as sub_commusers_peak,
         |       musers as users,
         |       mgtusers as gt_users,
         |       mvlusers as volte_users
         |  from (select ttime,
         |               hours,
         |               cellid,
         |               pluem,
         |               count(sub_pulse_mark) as c1,
         |               min(sub_pulse_mark) as m1,
         |               max(comm_users) as scu,
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
         |                       comm_users,
         |                       volte_users
         |                  from gt_pulse_cell_min
         |                 where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         |                 and sub_pulse_type = 1) a
         |         group by ttime, hours, cellid, pluem) b
       """.stripMargin).createOrReplaceTempView("gt_pulse_cell_base60_tmp")
    sql(
      s"""
         | select
         |       pct.ttime as ttime,
         |       pct.hours as hours,
         |       pct.cellid as cellid,
         |       pct.pulse_mark as pulse_mark,
         |       1 as pulse_type,
         |       count(bct.sub_pulse_mark) as pulse_timelen ,
         |       min(pct.first_pulse_mark) as first_pulse_mark,
         |       max(pct.users)  as sub_users_peak,
         |       max(pct.gt_users) as sub_gtusers_peak,
         |       max(pct.volte_users) as sub_volteusers_peak,
         |       max(pct.sub_commusers_peak) as sub_commusers_peak,
         |       count(distinct bct.imsi) as users,
         |       count(distinct case when bct.gtuser_flag = 1 then 1 else 0 end) as gt_users,
         |       count(distinct case when bct.volteuser_flag =1 then 1 else 0 end) as volte_users
         |  from gt_pulse_cell_base60_tmp pct
         | inner join
         |    (select gpd.cellid,gpd.imsi,gpd.gtuser_flag,gpd.volteuser_flag,gpc.sub_pulse_mark from gt_pulse_detail gpd inner join gt_pulse_cell_min gpc on gpd.sub_pulse_mark = gpc.sub_pulse_mark where gpd.dt="$ANALY_DATE" and gpd.h="$ANALY_HOUR" and gpd.cellid = gpc.cellid ) as bct
         |    on pct.cellid = bct.cellid
         | group by pct.ttime,pct.hours,pct.cellid, pct.pulse_mark
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
