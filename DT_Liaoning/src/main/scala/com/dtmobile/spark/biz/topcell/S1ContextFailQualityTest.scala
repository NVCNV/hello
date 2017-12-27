package com.dtmobile.spark.biz.topcell

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * S1初始上下文建立失败TOP小区
  *
  * @param analyDate 分析日期
  * @param dayNum 对比前几天的数据
  * @param contextSuccRate 成功率
  * @param contextFail 失败次数
  */
class S1ContextFailQualityTest(analyDate:String,dayNum:Int,zcsum:Integer,contextSuccRate:Double,contextFail:Int,dcl:String) {
  def analyse(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.sql
    var date=""
    var dayTmp=""
    var exDate = ""
    val warhouseDir = "/user/hive/warehouse/liaoning.db/"
    //根据天数动态拼接查询天数
    for(i <- 1 to dayNum){
      if(dayTmp == ""){
        dayTmp = analyDate
      }else{
        dayTmp  = getYesterday(dayTmp)
      }
      date += " vgu.dt = "+getYesterday(dayTmp) +" or"
      exDate += " ex.dt = " +getYesterday(dayTmp) + " or"
    }
    date = date.substring(0,date.length-2)
    exDate = exDate.substring(0,exDate.length-2)
    sql(
      s"""
        |select ex.*
        |          from ${dcl}.exception_analysis ex
        |         inner join (select cellid
        |                      from (select cellid,
        |                                   count(1),
        |                                   sum(case
        |                                         when hourcount >= ${zcsum} then
        |                                          1
        |                                         else
        |                                          0
        |                                       end) as daycount
        |                              from (select cellid, dt, count(1) as hourcount
        |                                      from ${dcl}.volte_gt_cell_ana_base60_t vgu
        |                                     where (${date})
        |                                       and (vgu.h = 08 or vgu.h = 09 or
        |                                           vgu.h = 10 or vgu.h = 11 or
        |                                           vgu.h = 12 or vgu.h = 13 or
        |                                           vgu.h = 14 or vgu.h = 15 or
        |                                           vgu.h = 16 or vgu.h = 17 or
        |                                           vgu.h = 18 or vgu.h = 19 or
        |                                           vgu.h = 20 or vgu.h = 21 or
        |                                           vgu.h = 22)
        |                                       and (contextcount - contextsucc) >= ${contextFail}
        |                                    --and (contextsucc / contextcount) < ${contextSuccRate}
        |                                     group by cellid, dt) t1
        |                             group by cellid) t2
        |                     where daycount >= ${dayNum}) kpi
        |            on ex.cellid = kpi.cellid
        |         where ex.etype = 3
        |           and (${exDate})
        |           and (ex.h = 08 or ex.h = 09 or ex.h = 10 or ex.h = 11 or
        |               ex.h = 12 or ex.h = 13 or ex.h = 14 or ex.h = 15 or
        |               ex.h = 16 or ex.h = 17 or ex.h = 18 or ex.h = 19 or
        |               ex.h = 20 or ex.h = 21 or ex.h = 22)
      """.stripMargin).createOrReplaceTempView("context_tmp")

    sql(
      s"""
        |
        | select t4.cellid,
        |        t4.exceptioncode,
        |        t4.minname,
        |        t4.wirelessresultdesc,
        |        t4.exceptioncodecnt / t4.sumexceptioncode,
        |        t4.r1
        |   from (select t3.cellid,
        |                t3.exceptioncode,
        |                t3.exceptioncodecnt,
        |                t3.minname,
        |                t3.wirelessresultdesc,
        |                sum(t3.exceptioncodecnt) as sumexceptioncode,
        |                row_number() over(partition by t3.cellid, t3.exceptioncode order by t3.exceptioncodecnt desc) r1
        |           from (select t2.cellid,
        |                        t2.exceptioncode,
        |                        count(t2.exceptioncode) as exceptioncodecnt,
        |                        min(event_name) as minname,
        |                        min(wirelessresultdesc) as wirelessresultdesc
        |                   from context_tmp t2
        |                  group by t2.cellid, t2.exceptioncode) t3
        |          group by t3.cellid,
        |                   t3.exceptioncodecnt,
        |                   t3.exceptioncode,
        |                   t3.minname,
        |                   t3.wirelessresultdesc) t4
        |  where t4.r1 <= 3
      """.stripMargin).repartition(100).write.mode(SaveMode.Overwrite)
      .csv(s"$warhouseDir/test5/dt=20171226/h=15")

    sql(
      """
        | select t4.cellid,
        |        t4.falurecausecnt,
        |        t4.minname,
        |        t4.resultdesc,
        |        t4.falurecausecnt / t4.sumfalurecause,
        |        t4.r1
        |   from (select t3.cellid,
        |                t3.falurecause,
        |                t3.falurecausecnt,
        |                t3.minname,
        |                t3.resultdesc,
        |                sum(t3.falurecausecnt) as sumfalurecause,
        |                row_number() over(partition by t3.cellid, t3.falurecause order by t3.falurecausecnt desc) r1
        |           from (select t2.cellid,
        |                        t2.falurecause,
        |                        count(t2.falurecause) as falurecausecnt,
        |                        min(event_name) as minname,
        |                        min(resultdesc) as resultdesc
        |                   from context_tmp t2
        |                  group by t2.cellid, t2.falurecause) t3
        |          group by t3.cellid,
        |                   t3.falurecausecnt,
        |                   t3.falurecause,
        |                   t3.minname,
        |                   t3.resultdesc) t4
        |  where t4.r1 <= 3
      """.stripMargin).repartition(100).write.mode(SaveMode.Append)
      .csv(s"$warhouseDir/test5/dt=20171226/h=15")
      sql(
        """
          | select t4.cellid,
          |        t4.minname,
          |        t4.cellregioncnt / t4.sumcellregion,
          |        t4.r1
          |   from (select t3.cellid,
          |                t3.cellregion,
          |                t3.cellregioncnt,
          |                t3.minname,
          |                sum(t3.cellregioncnt) as sumcellregion,
          |                row_number() over(partition by t3.cellid, t3.cellregion order by t3.cellregioncnt desc) r1
          |           from (select t2.cellid,
          |                        t2.cellregion,
          |                        count(t2.cellregion) as cellregioncnt,
          |                        min(event_name) as minname
          |                   from context_tmp t2
          |                  group by t2.cellid, t2.cellregion) t3
          |          group by t3.cellid,
          |                   t3.cellregioncnt,
          |                   t3.cellregion,
          |                   t3.minname
          |                   ) t4
          |  where t4.r1 <= 1
        """.stripMargin).repartition(100).write.mode(SaveMode.Append)
        .csv(s"$warhouseDir/test5/dt=20171226/h=15")
  }



  /**
    * 获取指定日期前一天
    *
    * @param date
    * @return
    */
  def getYesterday(date:String):String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val day:Date = dateFormat.parseObject(date).asInstanceOf[Date]
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(day)
    cal.add(Calendar.DAY_OF_MONTH, -1);
    val yesterdayTime:Date = cal.getTime
    val yesterday = dateFormat.format(yesterdayTime)
    yesterday
  }

}
