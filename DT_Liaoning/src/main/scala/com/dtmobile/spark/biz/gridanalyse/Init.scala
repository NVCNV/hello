package com.dtmobile.spark.biz.gridanalyse



import org.apache.spark.sql.SparkSession

/**
  * Created by shenkaili on 17-5-8.
  */
class Init(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String) {

  val oracle:String = "jdbc:oracle:thin:@"+ORCAL

  def analyse(implicit sparkSession: SparkSession): Unit = {
      InitLteCell(sparkSession)
      TableUtil(sparkSession)
      mrfilter(sparkSession)
      lte2lteadj_f(sparkSession)
      InDoorAna(sparkSession)
  }
    def mrfilter(sparkSession: SparkSession): Unit ={
      /*val CellDF = sparkSession.read
        .format("jdbc")
        .option("url", s"$oracle")
        .option("dbtable", "grid_view")
        .option("user", "scott")
        .option("password", "tiger")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load().createOrReplaceTempView("grid_view")*/
//            val t= sparkSession.sql("select * from grid_view2").count()
      import sparkSession.sql
      /*sql(
        s"""|SELECT t1.OBJECTID as objectid, t1.VID as vid, t1.STARTTIME as starttime, t1.ENDTIME as endtime, t1.mrtime as timeseq
           | , t1.ENBID as enbid, t1.MRNAME as mrname, t1.CELLID as cellid, t1.MMEUES1APID as mmeues1apid, t1.MMEGROUPID as mmegroupid
           | , t1.MMECODE as mmecode, t1.MEATIME as meatime, t2.shapeminx as gridcenterlongitude,t2.shapemaxy as gridcenterlatitude, t1.KPI1 as kpi1
           | , t1.KPI2 as kpi2, t1.KPI3 as kpi3, t1.KPI4 as kpi4, t1.KPI5 as kpi5, t1.KPI6 as kpi6
           | , t1.KPI7 as kpi7, t1.KPI8 as kpi8, t1.KPI9 as kpi9, t1.KPI10 as kpi10, t1.KPI11 as kpi11
           | , t1.KPI12 as kpi12, t1.KPI13 as kpi13, t1.KPI14 as kpi14, t1.KPI15 as kpi15, t1.KPI16 as kpi16
           | , t1.KPI17 as kpi17, t1.KPI18 as kpi18, t1.KPI19 as kpi19, t1.KPI20 as kpi20, t1.KPI21 as kpi21
           | , t1.KPI22 as kpi22, t1.KPI23 as kpi23, t1.KPI24 as kpi24, t1.KPI25 as kpi25, t1.KPI26 as kpi26
           | , t1.KPI27 as kpi27, t1.KPI28 as kpi28, t1.KPI29 as kpi29,t2.OBJECTID as OID
           |FROM $SDB.lte_mro_source t1
           |left join $DDB.grid_view t2 WHERE t1.mrname = 'MR.LteScRSRP' and t1.GRIDCENTERLONGITUDE > t2.shapeminx and t1.GRIDCENTERLONGITUDE < t2.shapemaxx
           |            and t1.GRIDCENTERLATITUDE > t2.shapeminy
           |            and t1.GRIDCENTERLATITUDE < t2.shapemaxy and ((ROUND(t1.GRIDCENTERLONGITUDE,2) = t2.x
           |            and ROUND(t1.GRIDCENTERLATITUDE,2) =  t2.y)   or  (ROUND(t1.GRIDCENTERLONGITUDE,2) = t2.x1
           |            and ROUND(t1.GRIDCENTERLATITUDE,2) =  t2.y1)) and t1.dt="$ANALY_DATE" and t1.h="$ANALY_HOUR"
         """.stripMargin).show*/
      /* val t = sql(s"""select * from grid_view2 t1 ,(select max(gridcenterlongitude) as maxlongitude,max(gridcenterlatitude) as maxlatitude,
            | min(gridcenterlongitude) as minlongitude,min(gridcenterlatitude) as minlatitude
            | from lte_mro_source where dt="20170405" and  h="15")t2 where t1.shapeminx >= t2.minlongitude and t1.shapemaxx<t2.maxlongitude and
            | t1.shapeminy<t2.maxlongitude and t1.shapemaxy<t2.maxlatitude """.stripMargin)
        print("----------------------"+t+"--------------------------------------")*/

     sql(
        s"""|SELECT x1.OBJECTID as objectid, x1.VID as vid, x1.STARTTIME as starttime, x1.ENDTIME as endtime, x1.mrtime as timeseq
            |, x1.ENBID as enbid, x1.MRNAME as mrname, x1.CELLID as cellid, x1.MMEUES1APID as mmeues1apid, x1.MMEGROUPID as mmegroupid
            |, x1.MMECODE as mmecode, x1.MEATIME as meatime, x2.gridcenterlongitude,x2.gridcenterlatitude, x1.KPI1 as kpi1
            |, x1.KPI2 as kpi2, x1.KPI3 as kpi3, x1.KPI4 as kpi4, x1.KPI5 as kpi5, x1.KPI6 as kpi6
            |, x1.KPI7 as kpi7, x1.KPI8 as kpi8, x1.KPI9 as kpi9, x1.KPI10 as kpi10, x1.KPI11 as kpi11
            |, x1.KPI12 as kpi12, x1.KPI13 as kpi13, x1.KPI14 as kpi14, x1.KPI15 as kpi15, x1.KPI16 as kpi16
            |, x1.KPI17 as kpi17, x1.KPI18 as kpi18, x1.KPI19 as kpi19, x1.KPI20 as kpi20, x1.KPI21 as kpi21
            |, x1.KPI22 as kpi22, x1.KPI23 as kpi23, x1.KPI24 as kpi24, x1.KPI25 as kpi25, x1.KPI26 as kpi26
            |, x1.KPI27 as kpi27, x1.KPI28 as kpi28, x1.KPI29 as kpi29,
            |x2.OBJECTID as OID
            |from $SDB.lte_mro_source x1
            |left join
            |(select s1.gridcenterlongitude,s1.gridcenterlatitude,s2.OBJECTID
            |from
            |(select distinct gridcenterlongitude,gridcenterlatitude
            |from $SDB.lte_mro_source
            |where dt=$ANALY_DATE  and  h=$ANALY_HOUR)s1,
            |(select * from $DDB.grid_view t1 ,
            |(select max(gridcenterlongitude) as maxlongitude,max(gridcenterlatitude) as maxlatitude,
            |min(gridcenterlongitude) as minlongitude,
            |min(gridcenterlatitude) as minlatitude
            |from $SDB.lte_mro_source
            |where dt=$ANALY_DATE  and  h=$ANALY_HOUR and gridcenterlatitude>0)t2
            |where t1.shapeminx >= t2.minlongitude and t1.shapemaxx<t2.maxlongitude and
            |t1.shapeminy>=t2.minlatitude and t1.shapemaxy<t2.maxlatitude)s2
            |where s2.shapeminx <= s1.gridcenterlongitude and s2.shapemaxx>s1.gridcenterlongitude and
            |s2.shapeminy<s1.gridcenterlatitude
            |and s2.shapemaxy>s1.gridcenterlatitude)x2
            |on x2.gridcenterlongitude=x1.gridcenterlongitude
            |and x2.gridcenterlatitude=x1.gridcenterlatitude
            |and x1.dt=$ANALY_DATE and x1.h=$ANALY_HOUR
         """.stripMargin).createOrReplaceTempView("lte_mro_source_ana_tmp")

        /*sql("select oid from lte_mro_source_ana_tmp").show(50)*/

    }

   def lte2lteadj_f(sparkSession: SparkSession): Unit ={
      import sparkSession.sql
      sql(
        s"""
           |SELECT a.mmeGroupId as mmegroupid, a.mmeId as mmeid, a.eNodeBId as enodebid,a.cellId, a.cellName as cellname
           | , c1.pci as pci, c1.freq1 as freq1, a.adjMmeGroupId as adjmmegroupid, a.adjMmeId as adjmmeid, a.adjENodeBId as adjenodebid
           | , a.adjCellName as adjcellname,a.adjCellId as adjcellid,c2.pci as adjpci, c2.freq1 as adjfreq1
           |FROM lte2lteadj a, ltecell c1, ltecell c2
           |WHERE a.cellId = c1.cellId
           | AND a.eNodeBId = c1.eNodeBId
           | AND a.adjCellId = c2.cellId
           | AND a.adjENodeBId = c2.eNodeBId
         """.stripMargin).createOrReplaceTempView("lte2lteadj_pci")
    }

    def InDoorAna(sparkSession: SparkSession): Unit ={
      import sparkSession.sql
      sql(
        s"""
           |SELECT MMEUES1APID, enbID, CELLID, MrCount, VARI_TA
           | , VARI_AOA, AVG_SRCRSRP, (CASE WHEN MrCount >= 10
           | AND VARI_TA < 1
           | AND VARI_AOA < 1 THEN 1 END) as InDoorFlag
           |FROM (SELECT MmeUeS1apId AS MMEUES1APID, enbID, cellID AS CELLID, COUNT(*) AS MrCount,
           |VARIANCE(CASE WHEN kpi5 * 16 < 20512 THEN kpi5 * 16 / 16 END) AS VARI_TA,
           |VARIANCE(CASE WHEN kpi7 >= 0 THEN kpi7 END) AS VARI_AOA, AVG(CASE WHEN kpi1 >= 0 THEN kpi1-14 END) AS AVG_SRCRSRP
           | FROM lte_mro_source_ana_tmp t
           | WHERE t.mmeues1apId > 0
           |  AND t.mrname = 'MR.LteScRSRP'
           |  AND vid = 0
           | GROUP BY t.enbID, t.cellid, t.mmeues1apId
           | )t1
         """.stripMargin).createOrReplaceTempView("Mr_InDoorAna_Temp")
    }
  def InitLteCell(sparkSession: SparkSession): Unit ={
    sparkSession.read
      .format("jdbc")
      .option("url", s"$oracle")
      .option("dbtable", "ltecell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("ltecell")
   /* sparkSession.sql("select * from ltecell").show()*/
  }

def TableUtil(sc: SparkSession): Unit ={

  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","lte2lteadj")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("lte2lteadj")

  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","ltecover_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("ltecover_degree_condition")


   sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","(select  ID,FIELD,OPERATOR, CAST( to_char(value,'S999.999') AS float ) VALUE from ltedisturb_degree_condition ) T")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("LTEDISTURB_DEGREE_CONDITION")

  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","ltemrsegment_config")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("ltemrsegment_config")

  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","ltemrsegment_type")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("ltemrsegment_type")

  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","lteadjcell_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("lteadjcell_degree_condition")

   sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","ltepci_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("ltepci_degree_condition")


  sc.read.format("jdbc").option("url", s"$oracle")
    .option("dbtable","ltepci_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().createOrReplaceTempView("ltepci_degree_condition")

  sc.sql(
    s"""
       |select cellid,freq1,pci,tcellid,tenbid,d from
       |(select cellid,freq1,pci,tcellid,tenbid,d,rank() over(partition by cellid,freq1,pci order by d ) as rank from
       |(
       |select t.cellid as cellid,a.cellid as tcellid,a.enodebid as tenbid,a.freq1,a.pci,a.latitude as alatitude,
       |(POWER(ABS(a.LATITUDE-t.latitude), 2) + POWER(ABS(a.LONGITUDE-t.longitude), 2)) as d,
       |a.longitude as alongitude,t.latitude as tlatitude,t.longitude as tlongitude
       |from LTECell t,ltecell a where t.cellid!=a.cellid and t.freq1=a.freq1 and t.pci=a.pci
       |)
       |) where rank = 1
     """.stripMargin).createOrReplaceTempView("fill_tenbid_tcellid")

}



}
