package com.dtmobile.spark.job

import com.dtmobile.spark.Analyse
import org.apache.spark.sql.SparkSession
import com.dtmobile.spark.biz.shanxikpi._
import com.dtmobile.spark.biz.nssp.NsspAnaly

/**
  * AnalyJob
  *
  * @author heyongjin
  * create 2017/03/02 11:10
  *  $ANALY_DATE $ANALY_HOUR liaoning lndcl $MASTER ifdayanaly
  **/
class AnalyJob(args: Array[String]) extends Analyse {
   val appName: String = this.getClass.getName
   val master: String = args(4)
   val sourceDir: String = args(6)
   val warhouseDir: String = "/user/hive/warehouse/" + args(3) + ".db"
//  override val warhouseDir: String = "/"+args(2)
  val onoff=args(7).toInt


   def analyse(implicit sparkSession: SparkSession): Unit = {
    val nsspAnaly = new NsspAnaly(args(0), args(1), args(2), args(3), sourceDir, warhouseDir)
    val kpiHourAnaly = new KpiHourAnaly(args(0), args(1), args(2), args(3), warhouseDir,onoff)
//    val kpibusinessHourAnaly = new KpibusinessHourAnaly(args(0), args(1), args(2), args(3), warhouseDir,onoff)
//    val kpibusinessDayAnaly = new KpibusinessDayAnaly(args(0), args(2), args(3), warhouseDir)
//    val exception=new businessexception(args(0),args(1), args(2), args(3), warhouseDir,args(5))
//    val typedetail=new businesstypedetail(args(0),args(1), args(2), args(3), warhouseDir)
//    kpibusinessHourAnaly.analyse
//    nsspAnaly.analyse
    kpiHourAnaly.analyse
//    exception.analyse
//    typedetail.analyse


//    val init = new Init(args(0), args(1), args(2), args(3),warhouseDir,args(5))
//    val overcover =new Overcover(args(0), args(1), args(2), args(3),warhouseDir)
//    val disturbAnalysis =new  DisturbAnalysis(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
//    val disturbMixAna =new DisturbMixAna(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
//    val disturbSecAna =new DisturbSecAna(args(0), args(1),"1","1",args(2),args(3),warhouseDir)
//    val lteMroAdjCoverAna =new LteMroAdjCoverAna(args(0), args(1),"1",args(2),args(3),warhouseDir)
//    val pCIOptimize = new PCIOptimize(args(0), args(1), args(2), args(3), warhouseDir)
//    val weakcover = new Weakcover(args(0), args(1), args(2), args(3), warhouseDir)
//    val gridCover = new GridCover(args(0), args(1), args(2), args(3), warhouseDir)
//
//
//     init.analyse
//     overcover.analyse
//     disturbAnalysis.analyse
//     disturbMixAna.analyse
//     disturbSecAna.analyse
//     lteMroAdjCoverAna.analyse
//    pCIOptimize.analyse
//    weakcover.analyse
//    gridCover.analyse


////伪基站
/*
    val finit = new FakeDataInit(args(0), args(1), args(2), args(3),warhouseDir,args(5))
    val fAnaly = new FakeDataAnaly(args(0),args(1), args(2), args(3), warhouseDir,args(5))

    if ( "1".equals(args(7)) ){
      finit.analyse
    }
    else if ( "0".equals(args(7)) ){
      fAnaly.analyse
    }
*/
//    if("03".equals(args(1))){
//    val kpiDayAnALY = new KpiDayAnaly(DateUtils.addDay(args(0), -1, "yyyyMMdd"), args(2), args(3), warhouseDir)
//      kpiDayAnALY.analyse
//      kpibusinessDayAnaly.analyse
//    }
 }
}
  object AnalyJob {
    def main(args: Array[String]): Unit = {
      val analyJob = new AnalyJob(args: Array[String])
      analyJob.exec()
    }
  }



