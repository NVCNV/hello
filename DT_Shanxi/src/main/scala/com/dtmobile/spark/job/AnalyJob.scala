package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoudehu on 2017/5/19/0019.
  */
class AnalyJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(4)
  override val sourceDir: String = args(6)
  val warhouseDir: String = "/user/hive/warehouse/" + args(3) + ".db"
  override def analyse(implicit sparkSession: SparkSession): Unit = {




  }

  object AnalyJob {
    def main(args: Array[String]): Unit = {
      val analyJob = new AnalyJob(args: Array[String])
      analyJob.exec()
    }
  }

}
