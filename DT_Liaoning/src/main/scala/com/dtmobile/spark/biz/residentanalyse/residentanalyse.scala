package com.dtmobile.spark.biz.residentanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by weiyaqin on 2017/10/31.
  */
class residentanalyse(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {

  def analyse(implicit sparkSession: SparkSession): Unit ={

    import sparkSession.sql
    val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"

    sql(s"use $DDB")
    sql(s"alter table T_RESIDENT_KPI add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |select "$cal_date",
         |z.line,
         |z.city,
         |sum(z.gtusers),
         |sum(z.gtvolteusers),
         |sum(z.procellnum),
         |sum(z.pubcellnum),
         |sum(z.protopubnum),
         |sum(z.protopronum),
         |sum(z.esrvcc)
         |from (
         |select region as line,city as city,count(distinct(t.imsi)) as gtusers, 0 as gtvolteusers, 0 as procellnum,
         |0 as pubcellnum, 0 as protopubnum, 0 as protopronum, 0 as esrvcc from
         |(select * from volte_gt_busi_user_data where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |inner join ltecell g on t.CELLID = g.cellid group by region,city
         |union all
         |select region as line,city as city, 0 as gtusers, count(distinct(t.imsi)) as gtvolteusers, 0 as procellnum,
         |0 as pubcellnum,0 as protopubnum, 0 as protopronum, 0 as esrvcc from
         |(select * from volte_gtuser_data where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |inner join ltecell g on t.CELLID = g.cellid group by region,city
         |union all
         |select region as line,city as city, 0 as gtusers, 0 as gtvolteusers, count(case when cell_id is not null then 1 end) as procellnum,
         |count(case when cell_id is null then 1 end) as pubcellnum, 0 as protopubnum, 0 as protopronum, 0 as esrvcc from
         |(select t.cellid, n.cell_id from (select * from tb_xdr_ifc_uu_new where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |left join (select distinct cell_id, cell_name from result.t_profess_net_cell) n
         |on t.cellid = n.cell_id group by t.cellid,n.cell_id) b1 inner join ltecell g on b1.CELLID = g.cellid group by region,city
         |union all
         |select g.region as line,g.city as city, 0 as gtusers, 0 as gtvolteusers, 0 as procellnum, 0 as pubcellnum,
         |count(0) as protopubnum, 0 as protopronum, 0 as esrvcc from
         |(select * from t_resident_switch where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |inner join ltecell g on t.SRCCELLID = g.cellid group by g.region,g.city
         |union all
         |select region as line,city as city, 0 as gtusers, 0 as gtvolteusers, 0 as procellnum, 0 as pubcellnum,
         |0 as protopubnum,count(0) as protopronum, 0 as esrvcc from
         |(select t.cellid,t.targetcellid from (select * from tb_xdr_ifc_uu_new where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |inner join (select distinct cell_id, cell_name from result.t_profess_net_cell) n
         |on t.cellid = n.cell_id
         |inner join (select distinct cell_id, cell_name from result.t_profess_net_cell) m
         |on t.targetcellid = m.cell_id
         |where t.proceduretype in (7, 8)) a1 inner join ltecell g on a1.CELLID = g.cellid group by region,city
         |union all
         |select region as line,city as city, 0 as gtusers, 0 as gtvolteusers, 0 as procellnum, 0 as pubcellnum,
         |0 as protopubnum, 0 as protopronum, nvl(sum(t.SRVCCATT), 0) as esrvcc from
         |(select * from volte_gt_cell_ana_base60 where dt=$ANALY_DATE and h=$ANALY_HOUR) t
         |inner join ltecell g on t.CELLID = g.cellid group by region,city
         |) z group by z.line,z.city
         """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/T_RESIDENT_KPI/dt=$ANALY_DATE/h=$ANALY_HOUR/")
  }
}
