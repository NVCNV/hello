package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangchao15 on 2017/5/3.
  */
class LteMroAdjCoverAna(ANALY_DATE: String, ANALY_HOUR: String,  anahour: String,SDB: String, DDB: String, warhouseDir: String){

  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val cal_date2 = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR.toInt+1) + ":00:00"


  def analyse(implicit sparkSession: SparkSession): Unit = {
    lteMroAdjCoverAna(sparkSession)
  }

  def lteMroAdjCoverAna(sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    val t1 = sql("select value from ltepci_degree_condition where field = 'thrscgoodcoverrsrp'").collectAsList()
    var thrScGoodCoverRSRP : Int = -85
    if(t1.size()>0){
      thrScGoodCoverRSRP = t1.get(0).getInt(0)
    }
    val t2 = sql("select value from ltepci_degree_condition where field = 'thrnclackpoorcoverscrsrp'").collectAsList()
    var thrNcLackPoorCoverScRSRP : Int = -110
    if(t2.size() > 0){
      thrNcLackPoorCoverScRSRP = t2.get(0).getInt(0)+141
    }
    val t3 = sql("select value from ltepci_degree_condition where field = 'thrnclackpoorcoverncrsrp'").collectAsList()
    var thrNcLackPoorCoverNcRSRP : Int = -110
    if(t3.size > 0 ){
      thrNcLackPoorCoverNcRSRP = t3.get(0).getInt(0)+141
    }
    val t4 = sql("select value from ltepci_degree_condition where field = 'thrnclackpoorcoverrelrsrp'").collectAsList()
    var thrNcLackPoorCoverRelRSRP : Int = 3
    if(t4.size > 0){
      thrNcLackPoorCoverRelRSRP = t4.get(0).getInt(0)
    }
    sql(s"use $DDB")
    sql(s"""alter table lte_mro_adjcover_ana60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
      """)
    //INSERT
    //INTO lte_mro_adjcover_ana60 partition(dt='${ANALY_DATE}',h='${ANALY_HOUR}')
    // LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mro_adjcover_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR'
    sql(s"""
           |SELECT
           |       '' as id,
           |        '$cal_date' as starttime,
           |       '$cal_date2' as endtime,
           |       $ANALY_HOUR as timeseq ,
           |       s.MmeGroupId,
           |       s.Mmecode,
           |       s.enbID,
           |       s.cellID,
           |       SUM(
           |           CASE
           |             WHEN  s.kpi1 < ${thrNcLackPoorCoverScRSRP} AND (p.adjcellID IS NULL OR p.adjcellID = 0) AND s.kpi2 > ${thrNcLackPoorCoverNcRSRP} AND (s.kpi2 - s.kpi1) > ${thrNcLackPoorCoverRelRSRP} THEN 1
           |             ELSE 0 END ),
           |       SUM(
           |           CASE
           |            WHEN SIGN(s.kpi1 - ${thrNcLackPoorCoverScRSRP}) = -1 THEN 1
           |            ELSE  0 END ),
           |       SUM(
           |           CASE
           |            WHEN s.kpi1 > ${thrScGoodCoverRSRP} and s.kpi1 > s.kpi2 AND (s.kpi1 - s.kpi2) < ${thrScGoodCoverRSRP} THEN 1
           |            ELSE 0 END
           |       ),
           |       SUM(
           |           CASE
           |            WHEN SIGN(s.kpi1 - ${thrScGoodCoverRSRP}) = 1 THEN 1
           |            ELSE  0 END )
           |  FROM lte_mro_source_ana_tmp s
           |  left join lte2lteadj_pci p
           |    ON p.eNodeBId = s.enbID
           |   AND p.cellID = s.cellId
           |   AND p.adjPci = s.kpi12
           | WHERE s.kpi10 >= 0
           |   AND s.kpi12 >= 0
           | GROUP BY s.MmeGroupId, s.Mmecode, s.enbID, s.cellID
      """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mro_adjcover_ana60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
