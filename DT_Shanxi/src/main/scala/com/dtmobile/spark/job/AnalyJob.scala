package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import com.dtmobile.spark.biz.InitTable
import org.apache.spark.sql.SparkSession

/**
  * AnalyJob
  *
  * @author heyongjin
  * create 2017/03/02 11:10
  *  $ANALY_DATE $ANALY_HOUR liaoning lndcl $MASTER ifdayanaly
  **/
class AnalyJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(4)
  override val sourceDir: String = args(6)
  val warhouseDir: String = "/user/hive/warehouse/" + args(3) + ".db"


  override def analyse(implicit sparkSession: SparkSession): Unit = {

    val t = new InitTable(args(0),args(1),args(3),args(4),warhouseDir)


  }
}

object AnalyJob {
  def main(args: Array[String]): Unit = {
    val analyJob = new AnalyJob(args: Array[String])
    analyJob.exec()
  }
}
