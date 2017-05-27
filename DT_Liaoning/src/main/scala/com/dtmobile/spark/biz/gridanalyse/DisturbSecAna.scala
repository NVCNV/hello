package com.dtmobile.spark.biz.gridanalyse

import com.dtmobile.spark.biz.gridanalyse.Init
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.control.Breaks._

/**
  * Created by zhangchao15 on 2017/5/8.
  */
class DisturbSecAna(ANALY_DATE: String, ANALY_HOUR: String, anahour: String,period:String, SDB: String, DDB: String, warhouseDir: String) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  val cal_date2 = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR.toInt+1) + ":00:00"

  def analyse(implicit sparkSession: SparkSession): Unit = {
    lteMroAdjCoverAna(sparkSession)
  }
  def lteMroAdjCoverAna(sparkSession: SparkSession): Unit ={
    import sparkSession.sql

    var sqlSecSrv : String =s"SELECT  '','$cal_date' as starttime ,'$cal_date2' as endtime,${period} as period,$ANALY_HOUR as timeseq ,c.MmeGroupId,c.Mmeid,s.enbID,s.cellID,c.CellName,s.kpi10,s.kpi9,'MR.LteScRSRP'";
    var sqlSecAdj : String = s"SELECT  '','$cal_date' as starttime ,'$cal_date2' as endtime,${period} as period,$ANALY_HOUR as timeseq ,p.adjMmeGroupId,p.adjMmeId,p.adjenodebId,p.adjcellID,p.adjCellName,p.adjpci,p.adjfreq1,'MR.LteNcRSRP'";
    //var sqlSecTab : String = s"insert into lte_mro_disturb_sec partition(dt='${ANALY_DATE}',h='${ANALY_HOUR}')";
    val fString : String = "-120;-110;-100;-90;-80;-70;-60;"; //暂时写死 分段字符串
    val fDelimiter = ";"
    val cnt : Integer = fString.split(fDelimiter).length
    var min : Integer = 0
    var max : Integer = 0
    var i : Int = 0
    for ( i <- 0 to cnt ){
      //sqlSecTab += " ,seq" + i
      breakable{
        if(i == 0){
          break
        }
        min = max
        max = null2Zero(Integer.parseInt(getSplitString(fString,fDelimiter,i)))
        sqlSecSrv += " ,sum( CASE WHEN s.kpi1 >= "+min+" AND s.kpi1 < "+max+" THEN 1 ELSE 0 END  )"
        sqlSecAdj += " ,sum( CASE WHEN s.kpi2 >= "+min+" AND s.kpi1 < "+max+" THEN 1 ELSE 0 END  )"
      }
    }
    // sqlSecSrv += ",'','','','','','','','','','','','','','','','','','', '','','','','','','','','','','','','','','','','','', '','','','','','',  '','','','','','','','','','','','','','','','','','','','','','','','','','','','',''"
    sqlSecSrv += s" FROM (SELECT t.enbID,t.cellID,t.meaTime,t.kpi1,t.kpi9,t.kpi10 FROM lte_mro_source_tmp t WHERE t.VID = 0 ) s  LEFT JOIN ltecell c ON s.enbID = c.enodebId AND s.CellID = c.cellId WHERE 1=1 GROUP BY s.enbID,s.cellID,c.MmeGroupId,c.Mmeid,c.CellName,s.kpi10,s.kpi9"
    sqlSecAdj += s" FROM lte_mro_source_tmp s,lte2lteadj_pci p WHERE p.eNodeBId =s.enbID AND p.cellID = s.cellId AND p.adjPci = s.kpi12 AND P.ADJFREQ1 = s.kpi11 GROUP BY p.adjENodeBId,p.adjCellId ,p.adjMmeGroupId,p.adjMmeId,p.adjCellName,p.adjpci,p.adjfreq1 "
    //sqlSecTab+= " )"

    sql(s"use $DDB")
    sql(s"""alter table lte_mro_disturb_sec add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           LOCATION 'hdfs://dtcluster/$warhouseDir/lte_mro_disturb_sec/dt=$ANALY_DATE/h=$ANALY_HOUR'
      """)
    sql(s"""
        ${sqlSecSrv}
      """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/lte_mro_disturb_sec/dt=$ANALY_DATE/h=$ANALY_HOUR")
    sql(s"""
        ${sqlSecAdj}
      """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/lte_mro_disturb_sec/dt=$ANALY_DATE/h=$ANALY_HOUR")

  }

  def getSplitString(str : String , delimiter : String ,order : Integer) : String={
    var reStr : String = ""
    if(!str.contains(delimiter)){
      reStr = str
    }else if(order == 1){
      reStr = str.substring(0,str.indexOf(";")-1)
    }else{
      val posPre = findStrPos(str,delimiter,order-1)+1
      val posNext = findStrPos(str,delimiter,order)
      if("1".equals(posPre)){
        reStr = "1";
      }else if ("2".equals(posNext)){
        reStr = str.substring(posPre)
      }else{
        reStr = str.substring(posPre,posNext - posPre)
      }
    }
    reStr
  }

  def null2Zero(num : Integer) : Integer={
    var reNum :Integer = 0
    if(num == null){
      reNum = 0
    }else{
      reNum = num
    }
    reNum
  }

  /**
    * 取得字符 在字符串里的位置
    *
    * @param str
    * @return
    */
  def findStrPos(str : String,s : String,position : Int): Int ={
    var pos : Int = 0
    var count :Int  = 0
    for ( i <- 0 to str.length ){
      //        println(s+"\t\t\t\t\t" +str(i))
      if (s.trim == str(i).toString) {
        count = count + 1
      }
      if (position == count) {
        pos = i
        break()
      }
    }
    pos
  }
}

