package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/9/0009.
  */



class PCIOptimize(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String)  {

  def GenDisPreRsrpDifSeqValueSql(RSRP:String,TRSRP:String): String ={
    var sql = ""
    for (i<- -13 to 26){
      if(i < -12){
        sql += s"sum(case when  $RSRP - $TRSRP < -12 then 1 else 0 end),"
      }else if(i >= -12 && i <= 25){
        sql += s"sum(case when  $RSRP - $TRSRP = $i  then 1 else 0 end),"
      }else if(i > 25){
        sql +=s"sum(case when $RSRP - $TRSRP > 25 then 1 else 0 end)"
      }
    }
    sql
   }

  def GenDisPreRsrpDifSeqSql(): String = {
    var DifSeqSql = ""
    for (i <- -13 to 26) {
      if (i < -12) {
        DifSeqSql += "RSRPDIFSEQLS12,"
      } else if (i >= -12 && i <= 0) {
        var t = Math.abs(i)
        DifSeqSql += s"RSRPDIFSEQN$t,"
      } else if (i > 0 && i <= 25) {
        DifSeqSql += s"RSRPDIFSEQP$i,"
      } else if (i > 25) {
        DifSeqSql += s"RSRPDIFSEQMO25)"
      }
    }
    DifSeqSql
  }


//    val genDisPreRsrpDifSeqSql=GenDisPreRsrpDifSeqSql

  def analyse(implicit sparkSession: SparkSession): Unit= {
    import sparkSession.sql
    val genDisPreRsrpDifSeqValueSql = GenDisPreRsrpDifSeqValueSql("t.kpi1","t.kpi2")
    val t = sql("select operator,value from ltepci_degree_condition where field = 'cellrsrpcoverth'").collectAsList()
    val sRsrpLap = t.get(0).getString(0)+" "+t.get(0).getDecimal(1)
    val t2 = sql("select operator,value from ltepci_degree_condition where field = 'adjcellrsrpeffectiveth'").collectAsList()
    val adjRsrpthresh = t.get(0).getString(0)+" "+t.get(0).getDecimal(1)
    sql(s"use $DDB")
    sql(s"""alter table LTE_MRO_DISTURB_PRETREATE60 drop if  exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""".stripMargin)
    sql(s"""alter table LTE_MRO_DISTURB_PRETREATE60 add  partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""".stripMargin)
    val selectSql=s"""select t.startTime, t.endTime, t.timeseq,
                      |        t.mmecode, t.enbid, t.cellid,t2.cellname,
                      |        (case when t.kpi10!= -1 then t.kpi10 else null end) as cellpci,
                      |        (case when t.kpi9!= -1  then t.kpi9  else null end) as cellfreq,
                      |        t2.adjenodebid,t2.adjcellID,t2.adjcellname,
                      |        (case when t.kpi12!= -1 then t.kpi12 else null end) as tcellpci,
                      |        (case when t.kpi11!= -1 then t.kpi11 else null end) as tcellfreq,
                      |        sum(case when t.kpi1 >=0 then t.kpi1 - 141 else 0 end) as CELLRSRPSum,
                      |        sum(case when t.kpi1 >=0 then 1 else 0 end) as CELLRSRPCount,
                      |        sum(case when t.kpi2 >=0 then t.kpi2 - 141 else 0 end) as TCELLRSRPSum,
                      |        sum(case when t.kpi2 >=0 then 1 else 0 end) as TCELLRSRPCount,$genDisPreRsrpDifSeqValueSql
                      |        from (select * from lte_mro_source_ana_tmp where startTime is not null and mrname='MR.LteScRSRP'
                      |		and kpi1>=0 and kpi2>=0 and kpi1-141 $sRsrpLap
                      |        and kpi2-141 $adjRsrpthresh) T
                      |        left join lte2lteadj_pci T2 on T.cellId = T2.cellid and T2.adjpci = T.kpi12 and T2.adjfreq1 = T.kpi11
                      |		left join fill_tenbid_tcellid  f where f.cellid=t.cellid
                      |        group by t.startTime, t.endTime, t.timeseq,t.mmecode, t.enbid, t.cellid, t2.cellname,t2.adjenodebid,
                      |        t2.adjcellID, t2.adjcellname,t.kpi11, t.kpi12,t.kpi9, t.kpi10""".stripMargin

    sql(selectSql).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/LTE_MRO_DISTURB_PRETREATE60/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }

}
