package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import com.dtmobile.spark.biz.kpi.{KpiDayAnaly, KpiHourAnaly, KpibusinessDayAnaly, KpibusinessHourAnaly}
import com.dtmobile.spark.biz.nssp.NsspAnaly
import com.dtmobile.util.DateUtils
import org.apache.spark.sql.SparkSession
import com.dtmobile.spark.biz.businessexception.businessexception
import com.dtmobile.spark.biz.businesstypedetail.businesstypedetail


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
  override val sourceDir: String = args(2)
//  val warhouseDir1: String = "/user/hive/warehouse/" + args(3) + ".db"
  override val warhouseDir: String = "/"+args(2)

  override def analyse(implicit sparkSession: SparkSession): Unit = {
//    val nsspAnaly = new NsspAnaly(args(0), args(1), args(2), args(3), sourceDir, warhouseDir)
//    val kpiHourAnaly = new KpiHourAnaly(args(0), args(1), args(2), args(3), warhouseDir)
    val kpibusinessHourAnaly = new KpibusinessHourAnaly(args(0), args(1), args(2), args(3), warhouseDir)
    val kpibusinessDayAnaly = new KpibusinessDayAnaly(args(0), args(2), args(3), warhouseDir)
    val exception=new businessexception(args(0),args(1), args(2), args(3), warhouseDir)
    val typedetail=new businesstypedetail(args(0),args(1), args(2), args(3), warhouseDir)
    kpibusinessHourAnaly.analyse

    kpibusinessDayAnaly.analyse
    exception.analyse
    typedetail.analyse
//    nsspAnaly.analyse
//    kpiHourAnaly.analyse

    if("03".equals(args(1))){
//      val kpiDayAnALY = new KpiDayAnaly(DateUtils.addDay(args(0), -1, "yyyyMMdd"), args(2), args(3), warhouseDir)
//      kpiDayAnALY.analyse
    }
  }
}

object AnalyJob {
  def main(args: Array[String]): Unit = {
    val analyJob = new AnalyJob(args: Array[String])
    analyJob.exec()
  }
}
