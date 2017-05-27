package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2017/5/26.
  */
class PulseUserDetail(ANALY_DATE: String, ANALY_HOUR: String, DDB: String, warhouseDir: String) {
  def analyse(implicit sparkSession: SparkSession): Unit = {
    pulseUserDetail(sparkSession)
  }

  def pulseUserDetail(sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table gt_pulse_detail_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
                LOCATION 'hdfs://dtcluster/$warhouseDir/gt_pulse_detail_base60/dt=$ANALY_DATE/h=$ANALY_HOUR'
        """)

    sql(
      s"""
         |   select gpd.ttime,
         |         gpd.hours,
         |         gpd.cellid,
         |         gcb.imsi,
         |         gpd.pulse_mark,
         |         gpd.pulse_type,
         |         gpd.pulse_timelen,
         |         gpd.first_pulse_mark,
         |         gcb.gtuser_flag,
         |         gcb.volteuser_flag
         |  from(
         |  select gpd.ttime,
         |         gpd.hours,
         |         gpd.cellid,
         |          gcb.imsi,
         |         gpd.pulse_mark,
         |         gpd.pulse_type,
         |         gpd.pulse_timelen,
         |         gpd.first_pulse_mark,
         |         gcb.gtuser_flag,
         |         gcb.volteuser_flag
         |         row_number over(parition by gpd.cellid,gpd.pulse_mark,gcb.imsi) num
         |    from gt_pulse_detail gcb
         |    , gt_pulse_cell_base60 gpd
         |   where gcb.dt="$ANALY_DATE" and gcb.h="$ANALY_HOUR"
         |     and gcb.cellid = gpd.cellid and gcb.dt = gpd.dt and gcb.hours = gpd.hours
         |     and gcb.sub_pulse_mark >= gpd.first_pulse_mark
         |     and gcb.sub_pulse_mark < gpd.first_pulse_mark + gpd.pulse_timelen
         |     ) t where t.num=1
         |
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_detail_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
