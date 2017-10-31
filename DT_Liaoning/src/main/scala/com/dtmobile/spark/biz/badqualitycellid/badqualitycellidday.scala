package com.dtmobile.spark.biz.badqualitycellid

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by shenkaili on 2017/10/16.
  */
class badqualitycellidday(ANALY_DATE: String,SDB: String, DDB: String, warhouseDir: String){
  def analyse(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + "00:00:00"

    sql(s"alter table $DDB.ZC_CELL_DATA_DAY add if not exists partition(dt=$ANALY_DATE)")
    sql(s"use $DDB")
    sql(
      s"""
         |select
         |cellid,
         |(case when t.IMSIREGSUCC / t.IMSIREGATT<0.95 and (t.IMSIREGATT-t.IMSIREGSUCC)>50 then 4 else 0 end)IMSIREG,
         |(case when t.attachx / t.attachy <0.95 and attachy - attachx > 50 then 5 else 0 end)attachyfaile,
         |(case when t.tausucc / t.tauatt < 0.93 and tauatt - tausucc > 100 then 6 else 0 end)tauattfaile,
         |(case when t.lteswsucc / t.lteswatt <0.95 and (lteswatt - lteswsucc) > 100 then 7 else 0 end)lteswattfaile,
         |(case when t.wirelessdrop / t.wireless>0.02 and wireless - wirelessdrop > 80 then 8 else 0 end)wirelessfaile,
         |(case when t.srvccsuccs1 / t.srvccatt <0.98 and srvccatt - srvccsuccs1 > 30 then 10 else 0 end)srvccattfaile,
         |(case when (t.voltemcsucc + t.voltevdsucc) / (t.voltemcatt + t.voltevdatt) < 0.99 and
         |(t.voltemcatt + t.voltevdatt - t.voltemcsucc - t.voltevdsucc) > 30 then 11 else 0 end)voltefaile,
         |(case when (t.voltemchandover + t.voltevdhandover) /
         |(t.volteanswer + t.voltevdanswer)>0.02 and
         |(t.volteanswer + t.voltevdanswer - t.voltemchandover - t.voltevdhandover) > 30 then 12 else 0 end)voltemchandoverfaile,
         |(case when t.srqsucc / t.srqatt<0.98 and t.srqatt - t.srqsucc > 5 then 13 else 0 end)srqattfaile
         |from volte_gt_cell_ana_base60 t where dt=$ANALY_DATE
         """.stripMargin).createOrReplaceTempView("voltetmp")

    val weakcovernum_table= sql(
      s"""
         |select cellid,
         |(case when weakcoverratex / commy < 0.1 then 1 else 0 end)etype
         |from mr_gt_cell_ana_base60 where dt=$ANALY_DATE
       """.stripMargin)
    val highganlao_table=sql(
      s"""
         |select cellid,(case when (upsinrHighRatex/upsigrateavgy)>0.3 then 2 else 0 end)etype
         |from mr_gt_cell_ana_base60 where dt=$ANALY_DATE
       """.stripMargin)

    val IMSIREG_table=sql(
      s"""
         |select cellid,(case when IMSIREG=4 then IMSIREG else 0 end)etype from voltetmp
       """.stripMargin)
    val attachyfaile_table=sql(
      s"""
         |select cellid,(case when attachyfaile=5 then attachyfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val tauattfaile_table=sql(
      s"""
         |select cellid,(case when tauattfaile=6 then tauattfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val lteswattfaile_table=sql(
      s"""
         |select cellid,(case when lteswattfaile=7 then lteswattfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val wirelessfaile_table=sql(
      s"""
         |select cellid,(case when wirelessfaile=8 then wirelessfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val srvccattfaile_table=sql(
      s"""
         |select cellid,(case when srvccattfaile=10 then srvccattfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val voltefaile_table=sql(
      s"""
         |select cellid,(case when voltefaile=11 then voltefaile else 0 end)etype from voltetmp
       """.stripMargin)
    val voltemchandoverfaile_table=sql(
      s"""
         |select cellid,(case when voltemchandoverfaile=12 then voltemchandoverfaile else 0 end)etype from voltetmp
       """.stripMargin)
    val srqattfaile_table=sql(
      s"""
         |select cellid,(case when srqattfaile=13 then srqattfaile else 0 end)etype from voltetmp
       """.stripMargin)
    //===========================低速率占比=====================================
    val lowspeed_table= sql(
      s"""
         |select cellid,(case when (sum(case when gtuser_flag=1 then 1 else 0 end)/count(1))<0.5 then 3 else 0 end)etype from gt_pulse_detail where dt=$ANALY_DATE group by cellid
       """.stripMargin)





    IMSIREG_table.union(attachyfaile_table).union(tauattfaile_table).union(lteswattfaile_table).union(wirelessfaile_table).union(srvccattfaile_table)
      .union(voltefaile_table).union(voltemchandoverfaile_table).union(srqattfaile_table).union(weakcovernum_table).union(lowspeed_table).union(highganlao_table)
      .createOrReplaceTempView("untable")
//    sql(
//      s"""
//         |select "$cal_date",cellid,etype from untable
//       """.stripMargin).repartition(300).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/zc_cell_data_hour/dt=$ANALY_DATE/h=$ANALY_HOUR")


    sql(
      s"""
         |select "$cal_date",cellid,etype from untable where etype<> 0
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/zc_cell_data_day/dt=$ANALY_DATE/")
  }

}