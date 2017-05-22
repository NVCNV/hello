package com.dtmobile.spark.biz.businesstypedetail

import  org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * Created by shenkaili on 17-4-13.
  */
class businesstypedetail (ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String){


  val CAL_DATE = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"

  def analyse(implicit sparkSession: SparkSession): Unit = {
    typedetailAnalyse(sparkSession)

  }



  def typedetailAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"""alter table business_type_detail add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/business_type_detail/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
 /*
    sql(
      s"""
         |Select "$CAL_DATE",t1.city,t2.REGION,t1.ecgi, t1.apptype, t1.appsubtype, sum(uldata),sum(dldata) from tb_xdr_ifc_http t1
         |inner join ltecell t2 on t1.ecgi=t2.cellid
         |where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         |group by
         |t1.city,t2.REGION,t1.ecgi,t1.apptype, t1.appsubtype
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/business_type_detail/dt=$ANALY_DATE/h=$ANALY_HOUR")
*/
    sql(
      s"""
         | Select "$CAL_DATE",t1.city,t2.REGION,t1.apptype, t1.appsubtype, sum(uldata),sum(dldata),count(*),
         | sum(procedureendtime-procedurestarttime) from tb_xdr_ifc_http t1
         | inner join ltecell t2 on t1.ecgi=t2.cellid
         | where dt="$ANALY_DATE" and h="$ANALY_HOUR"
         | group by
         | t1.city,t2.REGION,t1.apptype, t1.appsubtype
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/business_type_detail/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}
