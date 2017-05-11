package com.dtmobile.spark.biz.fakedata

import org.apache.spark.sql.SparkSession

/**
  * Created by weiyaqin on 2017/5/2.
  */
class FakeDataAnaly(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String) {

  def analyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql

    sql(s"use $DDB")

    sql(s"""alter table TB_FAKE_DATA_TEMP add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/TB_FAKE_DATA_TEMP/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select t.starttime,t.endtime,t.meatime,t.enbid,t.cellid,t.gridcenterlongitude,t.gridcenterlatitude,t.kpi1,t.kpi2,t.kpi11,t.kpi12,
         | t.mmeues1apid,t.mmegroupid,t.mmecode,floor(c.dis)
         | from lte_mro_source t left join tb_cell_distance c on t.cellid = c.cellid and t.kpi12 = c.pci and t.kpi11 = c.freq1
         | where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         | and t.mrname = 'MR.LteScRSRP' and t.kpi2 - t.kpi1 >= 0 and t.kpi2 >= 0 and  t.eventtype <> 'PERIOD'
       """.stripMargin).createOrReplaceTempView("TB_FAKE_DATA_TEMP")

    //    insert into tb_fake_data (starttime,endtime,freq1,pci,cellnum,sessionnum,lon,lat,numcount)
    sql(s"""alter table TB_FAKE_DATA add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/TB_FAKE_DATA/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select starttime,endtime, kpi11 freq1,kpi12 pci,count(distinct t.cellid) cellnum,count(distinct mmeUEs1apid) sessionnum,
         | avg(c.longitude) lon,
         | avg(c.latitude) lat ,count(*) numcount
         | from tb_fake_data_temp t left join ltecell c on c.cellid = t.cellid
         | where (t.adjcellid is  null or t.adjcellid > 2) and kpi11 is not null
         | group by starttime,endtime, kpi11, kpi12  having count(*) >=150 and count(distinct t.cellid) >=10
       """.stripMargin).createOrReplaceTempView("TB_FAKE_DATA")

    //insert  /*+append*/   into tb_fake_effcell
    sql(s"""alter table TB_FAKE_EFFCELL add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/TB_FAKE_EFFCELL/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select tb.starttime, tb.endtime,tb.pci,tb.freq1,tb.cellid,tb.sessionnum,tb.kpi1,tb.kpi2 from
         | (
         | select starttime,endtime, kpi11 freq1,kpi12 pci,cellid,count(distinct mmeUEs1apid) sessionnum,avg(kpi1) kpi1,avg(kpi2) kpi2,
         | count(*) numcount from tb_fake_data_temp t
         | where (t.adjcellid is null or t.adjcellid >2 ) and kpi11 is not null
         | group by starttime,endtime, kpi11, kpi12 , cellid
         | ) tb
         | right join tb_fake_data c on tb.freq1 = c.freq1 and tb.pci = c.pci and tb.starttime=c.starttime
       """.stripMargin).createOrReplaceTempView("TB_FAKE_EFFCELL")
  }
}
