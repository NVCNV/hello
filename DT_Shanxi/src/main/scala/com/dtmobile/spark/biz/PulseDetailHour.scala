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
    val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " "

    sql(s"use $DDB")
    sql(
      s"""alter table gt_pulse_cell_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
                LOCATION 'hdfs://dtcluster/$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR'
        """)
    sql(
      s"""
         |select
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
         |  from (select
         |               hours,
         |               cellid,
         |               pluem,
         |               count(sub_pulse_mark) as c1,
         |               min(sub_pulse_mark) as m1,
         |               max(comm_users) as scu,
         |               max(users) as musers,
         |               max(gt_users) as mgtusers,
         |               max(volte_users) as mvlusers
         |          from (select
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
         |         group by hours, cellid, pluem) b
       """.stripMargin).createOrReplaceTempView("gt_pulse_cell_base60_tmp")
    /*sql(
      s"""
         | select
         |       concat('${cal_date}','','${ANALY_HOUR}',':','00',':','00'),
         |       pct.hours as hours,
         |       pct.cellid as cellid,
         |       pct.pulse_mark as pulse_mark,
         |       1 as pulse_type,
         |       min(pct.pulse_timelen) as pulse_timelen ,
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
         | group by pct.hours,pct.cellid, pct.pulse_mark
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")*/

    sql(
    s"""
       | select
         |            concat('${cal_date}','','${ANALY_HOUR}',':','00',':','00'),
         |            hours as hours,
         |            cellid as cellid,
         |            pulse_mark as pulse_mark,
         |            1 as pulse_type,
         |            min(pulse_timelen) as pulse_timelen ,
         |            min(first_pulse_mark) as first_pulse_mark,
         |            max(users)  as sub_users_peak,
         |            max(gt_users) as sub_gtusers_peak,
         |            max(volte_users) as sub_volteusers_peak,
         |            max(sub_commusers_peak) as sub_commusers_peak,
         |            count(imsi) as users,
         |            sum(case when gtuser_flag = 1 then 1 else 0 end) as gt_users,
         |            sum(case when volteuser_flag =1 then 1 else 0 end) as volte_users
         |   		from
         |      (select distinct pct.hours,pct.cellid,pct.pulse_mark,pct.first_pulse_mark,pct.pulse_timelen,pct.users,pct.gt_users,
         |      pct.volte_users,pct.sub_commusers_peak,gpd.imsi,gpd.gtuser_flag,gpd.volteuser_flag
         |       from gt_pulse_cell_base60_tmp pct
         |      left join
         |      (select * from gt_pulse_detail where dt="$ANALY_DATE" and h="$ANALY_HOUR") gpd
         |      on gpd.cellid=pct.cellid where gpd.sub_pulse_mark >= pct.first_pulse_mark and gpd.sub_pulse_mark < pct.first_pulse_mark + pct.pulse_timelen) t
         |      group by hours,cellid,pulse_mark
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_cell_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
