package com.dtmobile.spark.biz.excepitonsection

import java.util
import java.util.Collections

import org.apache.spark.sql.SparkSession

/**
  * Created by shenkaili on 2017/10/12.
  */
class exceptionsection(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String){
    def analyse(implicit sparkSession: SparkSession): Unit ={
      import sparkSession.sql
      val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
      //      "dpnetgrid", "脱网路段");
      //      "highdpmcgrid", "高掉话路段");
      //      "highdistgrid", "高干扰路段");
      //      "freqswgrid", "频繁切换路段");
      //      "weakcovergrid","弱覆盖路段");
      val gridMap =new com.dtmobile.spark.biz.excepitonsection.GetGridMap(ANALY_DATE,ANALY_HOUR,SDB,DDB,warhouseDir)
//      sparkSession.udf.register("getdpnetgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"dpnetgrid").get(gridid.toString))
//      sparkSession.udf.register("gethighdpmcgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"highdpmcgrid").get(gridid.toString))
//      sparkSession.udf.register("gethighdistgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"highdistgrid").get(gridid.toString))
//      sparkSession.udf.register("getfreqswgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"freqswgrid").get(gridid.toString))
//      sparkSession.udf.register("getweakcovergrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"weakcovergrid").get(gridid.toString))

      gridMap.getGridMap(sparkSession,"dpnetgrid")
      sql(
        s"""
           |select gridid,",",value,midgrid from griddatatable
       """.stripMargin).show(10)

      sql(
        s"""
           |select t2.value,t1.gridid,t2.midgrid from
           |(select gridid from result.exception_analysis where dt=20170724 and h=13  group by gridid having sum(case when ETYPE=14 or ETYPE=15 then 1 else 0 end)>2)t1
           |left join griddatatable t2 on t1.gridid=t2.gridid
         """.stripMargin).show(10)
//
//      sql(
//        s"""
//           |select "$cal_date",gethighdpmcgrid(gridid),"高掉话路段","highdpmcgrid",gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR group by gridid having having sum(case when ETYPE=5 or ETYPE=7 then 1 else 0 end)>3
//           |
//         """.stripMargin)
//
//
//      sql(
//        s"""
//           |select "$cal_date",gethighdistgrid(gridid),"高干扰路段","highdistgrid",gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR group by gridid having sum(case when upsinr<3 and upsinr is not null and upsinr<>''  then 1 else 0 end)>2 order by gridid
//           |
//         """.stripMargin).show(100)

//      sql(
//        s"""
//           |select "$cal_date",getfreqswgrid(gridid),"频繁切换路段","freqswgrid",gridid from exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR group by gridid having sum(case when upsinr<3 then 1 else 0 end)>2 order by gridid
//           |
//         """.stripMargin)
//
//      sql(
//        s"""
//           |select "$cal_date",getweakcovergrid(gridid),"弱覆盖路段","weakcovergrid",gridid from exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR group by gridid having sum(case when upsinr<3 then 1 else 0 end)>2 order by gridid
//           |
//         """.stripMargin)


    }
}
