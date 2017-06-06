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
         | SELECT DISTINCT
         | b.ttime,
         |  b.hours,
         |  a.line,
         |  a.city,
         |  a.scellid,
         |  a.dcellid,
         |  a.sfreq,
         |  a.dfreq,
         |  a.pairname,
         |  a.scellname,
         |  b.cellid bcellid,
         |  b.pulse_mark,
         |  b.first_pulse_mark,
         |  b.pulse_timelen,
         |  b.users,
         |  c.cellid ccellid,
         |  c.imsi simsi,
         |  d.cellid ddcellid,
         |  d.imsi dimsi
         | FROM
         |  gt_balence_pair a
         |  INNER JOIN
         |  (select ttime,hours,cellid,pulse_mark,first_pulse_mark,pulse_timelen,users from gt_pulse_cell_base60
         |  WHERE dt='$ANALY_DATE' AND h='$ANALY_HOUR'
         |  ) b ON a.scellid=b.cellid
         |  INNER JOIN
         |  (
         |    SELECT h.cellid,h.imsi,sub_pulse_mark FROM gt_pulse_detail h INNER JOIN gt_balence_pair i ON h.cellid = i.scellid
         |    WHERE dt='$ANALY_DATE' AND h='$ANALY_HOUR'
         |  ) c ON a.scellid=c.cellid
         |  INNER JOIN
         |  (
         |    SELECT j.cellid,j.imsi,sub_pulse_mark FROM gt_pulse_detail j INNER JOIN gt_balence_pair k ON j.cellid = k.dcellid
         |    WHERE dt='$ANALY_DATE' AND h='$ANALY_HOUR'
         |  ) d on a.dcellid=d.cellid
         | WHERE
         |   c.sub_pulse_mark >= b.first_pulse_mark AND c.sub_pulse_mark < (b.first_pulse_mark + pulse_timelen)
         | AND
         |   d.sub_pulse_mark >= b.first_pulse_mark AND d.sub_pulse_mark < ( b.first_pulse_mark + pulse_timelen )
         | ) e
         | ) f group by ttime,hours,line,city,scellid,dcellid,sfreq,dfreq,pulse_mark,first_pulse_mark,pulse_timelen,pairname,scellname
         | ) g
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/gt_pulse_load_balence60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}