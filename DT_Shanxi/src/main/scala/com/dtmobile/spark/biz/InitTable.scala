package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/19/0019.
  */
class InitTable(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  def analyse(implicit sparkSession: SparkSession): Unit = {
    init(sparkSession)
  }

  def init(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    sql(
      s"""alter table $DDB.VOLTE_USER_DATA add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$warhouseDir/VOLTE_USER_DATA/dt=$ANALY_DATE/h=$ANALY_HOUR'
       """.stripMargin)

    val t =sql(
      s"""select
         |$cal_date,
         |imsi,
         |from_unixtime(cast(round(procedurestarttime /1000) as bigint),'mm')  procedurestarttime,
         |from_unixtime(cast(round(procedureendtime /1000) as bigint),'mm') procedureendtime
         |from $SDB.tb_xdr_ifc_mw
         |where dt=$ANALY_DATE and h=$ANALY_HOUR
         |and Interface=14
         |and imsi is not null
         |group by imsi,procedurestarttime,procedureendtime"""
        .stripMargin).createOrReplaceTempView("VOLTE_USER_DATA")
      sql("select * from volte_user").write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/VOLTE_USER_DATA/dt=$ANALY_DATE/h=$ANALY_HOUR""")







  }

}
