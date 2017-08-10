package com.dtmobile.spark.biz.nssp

import com.dtmobile.spark.Analyse
import org.apache.spark.sql.SparkSession

/**
  * Created by shenkaili on 17-7-4.
  */
class TimeFilterJob(args: Array[String])extends Analyse{
    val appName: String = this.getClass.getName
    val master: String = args(4)
    val sourceDir: String = "/"+ args(5)
    val warhouseDir: String ="/"+ args(3)


  override def analyse(implicit sparkSession: SparkSession) = {
    val tf=new TimeFilters(args(0), args(1), args(2), args(3), sourceDir, warhouseDir)
    tf.analyse
  }

}
object TimeFilterJob{
  def main (args: Array[String] ): Unit = {
    val analyJob = new TimeFilterJob(args: Array[String])
    analyJob.exec()
  }

  }
