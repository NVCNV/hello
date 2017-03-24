package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import com.dtmobile.spark.biz.kpi.KpiHourAnaly
import com.dtmobile.spark.biz.nssp.NsspAnaly
import org.apache.spark.sql.SparkSession

/**
  * AnalyJob
  *
  * @author heyongjin
  * @create 2017/03/02 11:10
  *
  **/
class AnalyJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(args.length - 1)
  override val sourceDir: String = args(2)
  override val warhouseDir: String = "/user/hive/warehouse/"+args(3)+".db"

  override def analyse(implicit sparkSession: SparkSession): Unit = {
    sparkSession.sql("use lndcl")
    sparkSession.sql("show tables").show()
   //val nsspAnaly = new NsspAnaly(args(0),args(1),args(2),args(3),sourceDir,warhouseDir)
   //val kpiHourAnaly = new KpiHourAnaly(args(0),args(1),args(2),args(3),warhouseDir)
    //nsspAnaly.analyse(sparkSession)
   // kpiHourAnaly.analyse(sparkSession)
  }
}

object AnalyJob {
  def main(args: Array[String]): Unit = {
    val analyJob = new AnalyJob(args: Array[String])
    analyJob.exec()
  }
}
