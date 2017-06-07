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
         LOCATION 'hdfs://dtcluster/$sourceDir/TB_XDR_IFC_UU/$ANALY_DATE/$ANALY_HOUR'
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
         LOCATION 'hdfs://dtcluster/$sourceDir/LTE_MRO_SOURCE/$ANALY_DATE/$ANALY_HOUR'
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

 /* sql(
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
     """.stripMargin).createOrReplaceTempView("volte_gtuser_data")*/




  sql(
    s"""
       |alter table $DDB.VOLTE_GT_BUSI_USER_DATA drop if  exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
       """.stripMargin)
  sql(
    s"""
       |alter table $DDB.VOLTE_GT_FREE_USER_DATA drop if  exists  partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
       """.stripMargin)

  sql(
    s"""
       |alter table $DDB.VOLTE_GT_FREE_USER_DATA add if not exists  partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
       |location "/$sourceDir/FreeGtUser/$ANALY_DATE/$ANALY_HOUR"
   """.stripMargin)

  sql(
    s"""
       |alter table $DDB.VOLTE_GT_BUSI_USER_DATA add if not exists  partition(dt="$ANALY_DATE",h="$ANALY_HOUR")
       |location "/$sourceDir/BusinessGtUser/$ANALY_DATE/$ANALY_HOUR"
   """.stripMargin)


  val freeUser = sql(s"""select imsi,cellid,RANGETIME from $DDB.VOLTE_GT_FREE_USER_DATA where dt=$ANALY_DATE and h=$ANALY_HOUR""")
  val busiUser = sql(s"""select imsi,cellid,RANGETIME from $DDB.VOLTE_GT_BUSI_USER_DATA where dt=$ANALY_DATE and h=$ANALY_HOUR""")

  freeUser.union(busiUser).createOrReplaceTempView("volte_gtuser_data")


 uu.union(ueMr).createOrReplaceTempView("temp_uu_ueMr")

  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " "

  sql(s"""alter table $DDB.gt_pulse_detail drop if  exists partition(dt="$ANALY_DATE",h="$ANALY_HOUR")""".stripMargin)

  sql(s"""alter table $DDB.gt_pulse_detail add  partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""".stripMargin)

  sql(
    s"""
       |
       |select
       |(case when minutes >= 10 then concat('${cal_date}','',$ANALY_HOUR,':',minutes,':','00')
       |       else concat('${cal_date}','',$ANALY_HOUR,':0',minutes,':','00')
       |       end) ttime,
       |'$ANALY_HOUR' hours,
       |minutes,
       |cellid,
       |imsi,
       |imei,
       |(case when (select count(*) from temp_uu_ueMr t where imsi in(select imsi from volte_gtuser_data ))>0 then 1 else 0 end)  gtuser_flag,
       |(case when (select count(*) from temp_uu_ueMr t where imsi in(select imsi from volte_user_data ))>0 then 1 else 0 end)   volteuser_flag,
       |minutes sub_pulse_mark
       |from temp_uu_ueMr
       | group by minutes,CELLID,IMSI,IMEI
       |order by minutes desc
     """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"""$warhouseDir/gt_pulse_detail/dt=$ANALY_DATE/h=$ANALY_HOUR""")


}







}
