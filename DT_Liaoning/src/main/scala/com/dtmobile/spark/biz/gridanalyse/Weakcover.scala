package com.dtmobile.spark.biz.gridanalyse

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 17-5-2.
  */
class Weakcover(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String) {

//  field_name = 'PoorCoverageRSRPTh'
  var poorRSRPOp="<"
  var poorRSRPTh="-110"

//  GoodCoverageRSRPTh
  var goodRSRPOp=">="
  var goodRSRP="-80"

//  poorUEpowerTh
  var poorUEPowerOp="<"
  var poorUEPower="3"

//  bigUEtxrxTh
  var bigUEtxrxOp=">="
  var bigUEtxrx="100"

//  deviateAzimuthTh
  var azimuthOp=">"
  var azimuthTh="35"

  def analyse(implicit sparkSession: SparkSession): Unit={
    import sparkSession.sql
    sql(s"use $DDB")
    sql(
      s"""alter table lte_mrs_dlbestrow_ana60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
        """.stripMargin)
    sql(
      s"""
       select t1.startTime as startTime, t1.endTime as endTime, t1.timeseq as timeseq,
           t1.enbID as enodebid, t1.cellid as cellid,
          count(distinct (case when t1.MmeUeS1apId>0 then t1.MmeUeS1apId end)) as userCount,
          count(distinct (case when t1.MmeUeS1apId>0 and t2.InDoorFlag = 1 then t1.MmeUeS1apId end)) as idruserCount,
          sum(case when t1.kpi1 >=0 then t1.kpi1-141 else 0 end) as rsrpSum,
          sum(case when t2.InDoorFlag = 1 and t1.kpi1 >=0 then t1.kpi1-141 else 0 end) as idrrsrpSum,
          sum(case when t1.kpi1>=0 then 1 else 0 end) as rsrptotalcount,
          sum(case when t1.kpi1>=0 and t2.InDoorFlag = 1 then 1 else 0 end) as idrrsrptotalcount,
          sum(case when t1.kpi1>0 and t1.kpi1- 141$poorRSRPOp$poorRSRPTh then 1 else 0 end) as poolCoverCount,
          sum(case when t2.InDoorFlag = 1 and t1.kpi1>0  and t1.kpi1 - 141$poorRSRPOp$poorRSRPTh then 1 else 0 end) as idrpoolCoverCount,
          sum(case when t1.kpi1>0  and  t1.kpi1-141 $goodRSRPOp$goodRSRP then 1 else 0 end) as goodCoverCount,
          sum(case when t1.kpi6>=0 and t1.kpi6 - 23$poorUEPowerOp$poorUEPower then 1 else 0 end) as poorUEpowerCount,
          sum(case when t1.kpi6>=0 then 1 else 0 end) as phrtotalcount, sum(case when t1.kpi6 >= 0 then t1.kpi6 - 23 else 0 end) as phrsum,
          sum(case when t1.kpi5* 16 $bigUEtxrxOp$bigUEtxrx then 1 else 0 end) as bigUEtxrxCount,
          sum(case when t1.kpi5>=0 then 1 else 0 end) as timedifftotalcount,
          sum(case when t1.kpi5>=0 then t1.kpi5 * 16 else 0 end) as timediffsum,
          sum(case when t1.kpi7>=0 then 1 else 0 end) as AOAcount,
          sum(case when t1.kpi7>=0 then t1.kpi7/2 else 0 end) as AOAsum,
          sum(case when t1.kpi7>=0 and ABS(360-t1.kpi7/2-t3.azimuth)$azimuthOp$azimuthTh  then 1 else 0 end) as AOAdeviates
          from lte_mro_source_ana_tmp t1
          left join Mr_InDoorAna_Temp t2 on t1.enbid = t2.enbid and t1.cellid = t2.cellid and t1.MMEUES1APID = t2.MMEUES1APID
          left join  ltecell t3 on t1.cellid=t3.cellid   and t1.mrname='MR.LteScRSRP' and t1.vid = 0
          group by t1.startTime, t1.endTime, t1.timeseq,t1.enbID, t1.cellid
          """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mrs_dlbestrow_ana60/dt=$ANALY_DATE/h=ANALY_HOUR")

    sql(
      s"""alter table LTE_MRO_JOINUSER_ANA60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
        """.stripMargin)
    sql(
      s"""select s1.startTime,s1.endTime, s1.timeseq,s1.mmegroupid,s1.mmeid, s1.enodebid, s1.cellid, s1.mmeues1apId,s3.CELLMRCOUNT,
                     s1.rsrpSum,s1.rsrptotalcount,s1.poolCoverCount,s1.goodCoverCount,s2.rsrp from (select t.startTime as startTime,
                     t.endTime as endTime, t.timeseq as timeseq,t.mmegroupid as mmegroupid,
                     t.mmecode as mmeid, t.enbID as enodebid, t.cellid as cellid, t.mmeues1apId,
                     sum(case when t.kpi1>=0 then t.kpi1 - 141 else 0 end) as rsrpSum, sum(case when t.kpi1>=0 then 1 else 0 end) as rsrptotalcount,
                     sum(case when t.kpi1>0  and  t.kpi1 -141$poorRSRPOp$poorRSRPTh then 1 else 0 end) as poolCoverCount,
                     sum(case when t.kpi1>0  and  t.kpi1 -141$goodRSRPOp$goodRSRP then 1 else 0 end) as goodCoverCount
                     from lte_mro_source_ana_tmp t
                     where t.mmeues1apId>0 and t.mrname='MR.LteScRSRP'and vid = 0
                     group by t.startTime, t.endTime, t.timeseq, t.mmegroupid,t.mmecode, t.enbID, t.cellid, t.mmeues1apId) s1
                     left join (select t2.mmegroupid,t2.MmeCode as mmeid,
                     t2.cellID as cellid, t2.MmeUeS1apId as mmeues1apId, t2.kpi1 -141 as rsrp
                     from (SELECT mmegroupid,cellid,mmeues1apId,MmeCode,avg(kpi1)kpi1 FROM lte_mro_source_ana_tmp l
                     WHERE 1 = 1 and l.mrname='MR.LteScRSRP'
                     and l.MmeUeS1apId>0 and l.kpi1>=0
                     and l.startTime is not null and vid = 0 group by mmegroupid,cellid,MmeCode,MmeUeS1apId) t2 ) s2
                     on s1.mmegroupid = s2.mmegroupid and s1.cellid = s2.cellid and s1.mmeues1apId = s2.mmeues1apId
                     left join (SELECT startTime,endTime,timeseq,enbID,cellid,SUM (CASE WHEN kpi1>=0 THEN 1 ELSE  0  END) as CELLMRCOUNT
                     from lte_mro_source_ana_tmp where mrname='MR.LteScRSRP'and vid = 0 GROUP BY startTime,endTime,timeseq,enbID,cellid ) s3 ON s1.startTime = s3.startTime
                     AND s1.endTime = s3.endTime AND s1.timeseq = s3.timeseq AND s1.enodebid = s3.enbid AND s1.cellid = s3.cellid
        """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/LTE_MRO_JOINUSER_ANA60/dt=$ANALY_DATE/h=ANALY_HOUR")
  }

}
