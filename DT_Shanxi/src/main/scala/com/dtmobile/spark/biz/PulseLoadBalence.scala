package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by weiyaqin on 2017/5/27/0026.
  */
class PulseLoadBalence(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {

  def analy(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    sql(s"use $DDB")
    sql(s"""alter table gt_pulse_load_balence60 drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""")
    sql(s"""alter table gt_pulse_load_balence60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/gt_pulse_load_balence60/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select g.*,(case when susers=dusers then 0 else s_dusers/abs(susers-dusers) end) s_dratio from
         | (
         | select ttime,hours,line,city,pulse_mark,sfreq,scellname,scellid,first_pulse_mark,pulse_timelen,pairname,
         | count(distinct simsi) susers,count(distinct dimsi) dusers,sum(s_d) s_dusers from
         | (
         | select ttime,hours,line,city,scellid,dcellid,sfreq,dfreq,pulse_mark,first_pulse_mark,pulse_timelen,
         | pairname,scellname,simsi,dimsi,(case when simsi=dimsi then 1 else 0 end) s_d from
         | (
         | select distinct b.ttime,b.hours,a.line,a.city,a.scellid,a.dcellid,a.sfreq,a.dfreq,a.pairname,
         | a.scellname,b.cellid bcellid,b.pulse_mark,b.first_pulse_mark,b.pulse_timelen,b.users,c.cellid ccellid,
         | c.imsi simsi,d.cellid ddcellid,d.imsi dimsi
         | from gt_balence_pair a, gt_pulse_cell_base60 b,
         |  (select h.* from gt_pulse_detail h inner join gt_balence_pair i on h.cellid=i.scellid) c,
         |  (select j.* from gt_pulse_detail j inner join gt_balence_pair k on j.cellid=k.dcellid) d
         | where a.scellid=b.cellid and c.cellid=b.cellid and c.hours=b.hours
         | and b.dt="$ANALY_DATE" and b.h="$ANALY_HOUR" and c.dt="$ANALY_DATE" and c.h="$ANALY_HOUR"
         | and d.dt="$ANALY_DATE" and d.h="$ANALY_HOUR"
         | and (c.sub_pulse_mark>=b.first_pulse_mark and c.sub_pulse_mark<(b.first_pulse_mark+pulse_timelen))
         | and d.cellid=a.dcellid and d.hours = b.hours
         | and (d.sub_pulse_mark>=b.first_pulse_mark and d.sub_pulse_mark<(b.first_pulse_mark+pulse_timelen))
         | ) e
         | ) f group by ttime,hours,line,city,scellid,dcellid,sfreq,dfreq,pulse_mark,first_pulse_mark,pulse_timelen,pairname,scellname
         | ) g
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_load_balence60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}