package com.dtmobile.spark.biz.fakedata

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by weiyaqin on 2017/5/9.
  */
class FakeDataInit(ANALY_DATE: String,ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String, ORCAL: String)  {

  def calcdis(fLon:Float,fLat:Float,tLon:Float,tLat:Float):Double={
    val PI:Double = 3.14159265
    val EARTH_RADIUS:Double = 6378.137

    val A = fLat*PI/180 - tLat*PI/180
    val B = fLon*PI/180 - tLon*PI/180
    val S = 2 * scala.math.asin(scala.math.sqrt(scala.math.pow(scala.math.sin(A/2),2) +
      scala.math.cos(fLat*PI/180)*scala.math.cos(tLat*PI/180)*scala.math.pow(scala.math.sin(B/2),2)))
    S * EARTH_RADIUS
  }

  def analyse(implicit HiveContext: HiveContext): Unit = {

    HiveContext.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORCAL")
      .option("dbtable", "ltecell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().registerTempTable("ltecell")

        HiveContext.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORCAL")
          .option("dbtable","lte2lteadj")
          .option("user", "scott")
          .option("password", "tiger")
          .option("driver", "oracle.jdbc.driver.OracleDriver")
          .load().registerTempTable("lte2lteadj")


        import HiveContext.sql

    //    HiveContext.udf.register("calcdis", calcdis(_:Float,_:Float,_:Float,_:Float))
        HiveContext.udf.register("calcdis", (fLon:Float,fLat:Float,tLon:Float,tLat:Float)=>(2 * scala.math.asin(scala.math.sqrt(scala.math.pow(scala.math.sin((fLat*3.14159265/180 - tLat*3.14159265/180)/2),2) + scala.math.cos(fLat*3.14159265/180)*scala.math.cos(tLat*3.14159265/180)*scala.math.pow(scala.math.sin((fLon*3.14159265/180 - tLon*3.14159265/180)/2),2))))*6378.137)
        sql(s"use $DDB")
        sql(s"""truncate table tb_cell_distance""")
//        sql(s"""alter table tb_cell_distance drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""")
//        sql(s"""alter table tb_cell_distance add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
//        LOCATION 'hdfs://dtcluster/$warhouseDir/tb_cell_distance/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
///*
        sql(
          s"""
             | select cellid, freq1,pci,min(dis) as dis  from
             | (select b.cellid,freq1,pci,dis from
             | (select t.cellid as cellid,a.cellid as tcellid,a.freq1,a.pci,calcdis(a.longitude,a.latitude,t.longitude,t.latitude) as dis
             | from ltecell t,ltecell a where t.cellid!=a.cellid) b
             | left join lte2lteadj c on b.cellid=c.cellid and b.tcellid=c.adjcellid where c.adjcellid is null)
             | group by cellid,freq1,pci
        """.stripMargin).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "false").save(s"$warhouseDir/tb_cell_distance/") //dt=$ANALY_DATE/h=$ANALY_HOUR")
    //    """.stripMargin).registerTempTable("tb_cell_distance")
//*/
    sql(s"""truncate table lte2lteadj_pci""")
      sql(
        s"""
           | SELECT a.mmeGroupId,a.mmeId,a.eNodeBId,a.cellId,a.cellName,c1.pci,c1.freq1,a.adjMmeGroupId,a.adjMmeId,
           | a.adjENodeBId,a.adjCellId,a.adjCellName,c2.pci as adjpci,c2.freq1 as adjfreq1
           | from lte2lteadj a ,ltecell c1 ,ltecell c2 WHERE a.cellId = c1.cellId AND a.eNodeBId = c1.eNodeBId
           | and a.adjCellId = c2.cellId and a.adjENodeBId = c2.eNodeBId
          """.stripMargin).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "false").save(s"$warhouseDir/lte2lteadj_pci/") //dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
