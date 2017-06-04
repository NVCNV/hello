package com.dtmobile.spark.biz

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/23/0023.
  */
class CellStatistics(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String,sourceDir:String) {

  def analyse(implicit sparkSession: SparkSession): Unit = {
    cellStatistics(sparkSession)
  }

def cellStatistics(sparkSession: SparkSession): Unit ={
  import sparkSession.sql
  //todo 数据库
  sql(
    s"""alter table $DDB.tb_xdr_ifc_uu add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$sourceDir/tb_xdr_ifc_uu/$ANALY_DATE/$ANALY_HOUR'
       """.stripMargin)

  val uu =sql(
    s"""
       |select
       |IMSI,
       |IMEI,
       |CELLID,
       |RANGETIME,
       |minute(RANGETIME)  minutes
       | from  $DDB.tb_xdr_ifc_uu  where dt="$ANALY_DATE" and h="$ANALY_HOUR" and IMSI!=''
     """.stripMargin)
  //todo 修改数据库
  sql(
    s"""alter table $DDB.lte_mro_source add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         LOCATION 'hdfs://dtcluster/$sourceDir/lte_mro_source/$ANALY_DATE/$ANALY_HOUR'
       """.stripMargin)
  val ueMr = sql(
    s"""|
       |select
       |IMSI,
       |IMEI,
       |cellID CELLID,
       |from_unixtime(cast(round(mrtime /1000) as bigint),'yyyy-MM-dd HH:mm:ss') RANGETIME,
       |from_unixtime(cast(round(mrtime /1000) as bigint),'mm') minutes
       | from $DDB.lte_mro_source
       | where dt="$ANALY_DATE" and h="$ANALY_HOUR" and MRNAME = 'MR.LteScRSRP' and IMSI!=''
     """.stripMargin)

  //todo 修改分区
  sql(
    s"""
       |select
       |ttime,
       |hours,
       |imsi,
       |volte_start,
       |volte_end
       |from $DDB.volte_user_data
       |where dt="$ANALY_DATE" and h="$ANALY_HOUR"
       |
     """.stripMargin).createOrReplaceTempView("volte_user_data")

  sql(
    s"""alter table $DDB.volte_gtuser_data add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
       """.stripMargin)
  sql(
    s"""
       |select
       |ttime,
       |cellid,
       |imsi
       |from  $DDB.volte_gtuser_data
       |where dt="$ANALY_DATE" and h="$ANALY_HOUR"  and IMSI!=''
     """.stripMargin).createOrReplaceTempView("volte_gtuser_data")

     uu.union(ueMr).createOrReplaceTempView("temp_uu_ueMr")



  sql(s"""alter table $DDB.gt_pulse_detail drop if  exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")""".stripMargin)

  sql(s"""alter table $DDB.gt_pulse_detail add  partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""".stripMargin)

  sql(
    s"""
       |
        |select
       |rangetime  ttime,
       |hour(rangetime) hours,
       |minutes,
       |cellid,
       |imsi,
       |imei,
       |(case when (select count(*) from temp_uu_ueMr t where imsi in(select imsi from volte_gtuser_data ))>0 then 1 else 0 end)  gtuser_flag,
       |(case when (select count(*) from temp_uu_ueMr t where imsi in(select imsi from volte_user_data ))>0 then 1 else 0 end)   volteuser_flag,
       |minutes sub_pulse_mark
       |from temp_uu_ueMr
       | group by minutes,CELLID,IMSI,IMEI,RANGETIME
       |order by minutes desc
     """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_pulse_detail/dt=$ANALY_DATE/h=$ANALY_HOUR""")

}







}
