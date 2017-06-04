package com.dtmobile.spark.job

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dtmobile.spark.Analyse
import com.dtmobile.spark.biz._
import org.apache.spark.sql.SparkSession

/**
  * AnalyJob
  *
  * @author zhoudehu
  * create 2017/05/22 11:10
  *  $ANALY_DATE $ANALY_HOUR liaoning lndcl $MASTER ifdayanaly
  **/
class AnalyJob(args: Array[String]) extends Analyse {
  override val appName: String = this.getClass.getName
  override val master: String = args(4)
  override val sourceDir: String = args(6)
  val warhouseDir: String = "/user/hive/warehouse/" + args(3) + ".db"

  override def analyse(implicit sparkSession: SparkSession): Unit = {
    val cal_date = args(0).substring(0, 4) + "-" + args(0).substring(4).substring(0,2) + "-" + args(0).substring(6)

    //获取前一天的日期
    def getDaysBefore(dt: String):String = {
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date = dateFormat.parse(dt)
      val cal: Calendar = Calendar.getInstance()
      cal.setTime(date);
      cal.add(Calendar.DATE, - 1)
      val yesterday = dateFormat.format(cal.getTime())
      val lastDay = yesterday.replaceAll("-","")
      lastDay
    }

   var currentDate:String= args(0)
    val currentHour = args(1).toInt
    var lastHour = ""
    if( 0<currentHour && currentHour<10 ){
      lastHour = "0"+currentHour.-(1)
    }else if(currentHour==0){
      lastHour = "23"
      currentDate=getDaysBefore(cal_date)
    }else{
      lastHour =  currentHour.-(1)+""
    }


    val init =new Init(args(5))
    val volteUser =new VolteUser(args(0),args(1),args(2),args(3),warhouseDir,sourceDir)
    val cell = new CellStatistics(currentDate,lastHour,args(2),args(3),warhouseDir,sourceDir)
    val pulseLoadBalence =new PulseLoadBalence(currentDate,lastHour,args(2),args(3),warhouseDir)
    val pulseDetailHour = new  PulseDetailHour(currentDate,lastHour,args(3),warhouseDir)
    val pulseUserDetail = new PulseUserDetail(currentDate,lastHour,args(3),warhouseDir)
    val subPulseStatis = new SubPulseStatis(currentDate,lastHour,args(3),warhouseDir)

    init.analyse
    volteUser.analyse
    cell.analyse
    subPulseStatis.analyse
    pulseDetailHour.analyse
    pulseUserDetail.analyse
    pulseLoadBalence.analy



    //调用天级分析
    val lastDay = getDaysBefore(cal_date)
//       if(args(1)=="03"){

         val gtUserFreqDay = new GtUserFreqDay(lastDay,args(3),warhouseDir,args(5))
         val highAttachCellDay = new HighAttachCellDay(lastDay,args(3),warhouseDir,args(5))
         val gtCommUserCellBasedDay = new GtCommUserCellBasedDay(lastDay,args(3),warhouseDir)
         val balenceBaseDay = new BalenceBaseDay(lastDay,args(3),warhouseDir)
         val overTimeDay = new OverTimeDay(lastDay,args(3),warhouseDir)
         val shortTimeDay = new ShortTimeDay(lastDay,args(3),warhouseDir)

             gtUserFreqDay.analyse
             highAttachCellDay.analyse
             gtCommUserCellBasedDay.analy
             balenceBaseDay.analy
             overTimeDay.analyse
             shortTimeDay.analyse
//       }

  }
}

object AnalyJob {
  def main(args: Array[String]): Unit = {
    val analyJob = new AnalyJob(args: Array[String])
    analyJob.exec()
  }
}
