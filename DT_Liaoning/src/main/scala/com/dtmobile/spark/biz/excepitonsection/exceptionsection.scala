package com.dtmobile.spark.biz.excepitonsection

import java.util
import java.util.Collections

import org.apache.spark.sql.{SaveMode, SparkSession}

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
      sql(
        s"""
           |alter table $DDB.gt_quesroad_base60 drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         """.stripMargin)
      sql(
        s"""
           |alter table $DDB.gt_quesroad_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
         """.stripMargin)
      sql(
        s"""
           |alter table $DDB.exception_analysis add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
           |LOCATION
           |  '$warhouseDir/exception_analysis/dt=$ANALY_DATE/h=$ANALY_HOUR'
         """.stripMargin)
      sql(
        s"""
           |select * from(
           |select t4.gridid,t4.rrunum,t4.distance,ROW_NUMBER()OVER(PARTITION BY t4.gridid ORDER BY t4.distance)rownum from
           |(select gridid,min(distance)distance,rrunum from
           |(select t1.gridid,(power(t1.lat-t2.lon,2)+power(t1.lon-t2.lat,2))distance,t2.rrunum from $DDB.grid t1 cross join $DDB.T_PROFESS_NET_CELL t2)t3 group by gridid,rrunum)t4)t5 where rownum=1
         """.stripMargin).createOrReplaceTempView("gridrru")
      val gridMap =new com.dtmobile.spark.biz.excepitonsection.GetGridMap(ANALY_DATE,ANALY_HOUR,SDB,DDB,warhouseDir)
//      sparkSession.udf.register("getdpnetgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"dpnetgrid").get(gridid.toString))
//      sparkSession.udf.register("gethighdpmcgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"highdpmcgrid").get(gridid.toString))
//      sparkSession.udf.register("gethighdistgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"highdistgrid").get(gridid.toString))
//      sparkSession.udf.register("getfreqswgrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"freqswgrid").get(gridid.toString))
//      sparkSession.udf.register("getweakcovergrid", (gridid:Int)=>gridMap.getGridMap(sparkSession,"weakcovergrid").get(gridid.toString))

      gridMap.getGridMap(sparkSession,"dpnetgrid")
      sql(
        s"""
           |select gridid,value,midgrid,cnt from griddatatable
       """.stripMargin).show(10)

      sql(
        s"""
           |select "$cal_date",gridids,"dpnetgrid","脱网路段",t4.rrunum,t3.cnt,t5.lon,t5.lat from
           |(select (t2.value)gridids,t1.gridid,(t2.midgrid)midgrid,t2.cnt from
           |(select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR  group by gridid having sum(case when ETYPE=14 or ETYPE=15 then 1 else 0 end)>2 order by gridid)t1
           |left join griddatatable t2 on t1.gridid=t2.gridid)t3
           |left join gridrru t4 on t4.gridid=t3.midgrid
           |left join $DDB.grid t5 on t5.gridid=t3.midgrid
           |group by gridids,t4.rrunum,t3.cnt,t5.lon,t5.lat
         """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/gt_quesroad_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")

      println("=======================SUCCESS============================")
      gridMap.getGridMap(sparkSession,"highdpmcgrid")
      sql(
        s"""
           |select "$cal_date",gridids,"highdpmcgrid","高掉话路段",t4.rrunum,t3.cnt,t5.lon,t5.lat from
           |(select (t2.value)gridids,t1.gridid,(t2.midgrid)midgrid,t2.cnt from
           |(select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR  group by gridid having sum(case when ETYPE=5 or ETYPE=7 then 1 else 0 end)>3 order by gridid)t1
           |left join griddatatable t2 on t1.gridid=t2.gridid)t3
           |left join gridrru t4 on t4.gridid=t3.midgrid
           |left join $DDB.grid t5 on t5.gridid=t3.midgrid
           |group by gridids,t4.rrunum,t3.cnt,t5.lon,t5.lat
         """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/gt_quesroad_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")

      gridMap.getGridMap(sparkSession,"highdistgrid")
      sql(
        s"""
           |select "$cal_date",gridids,"highdistgrid","高干扰路段",t4.rrunum,t3.cnt,t5.lon,t5.lat from
           |(select (t2.value)gridids,t1.gridid,(t2.midgrid)midgrid,t2.cnt from
           |(select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR  group by gridid having sum(case when upsinr<3 and upsinr is not null then 1 else 0 end)>2 order by gridid)t1
           |left join griddatatable t2 on t1.gridid=t2.gridid)t3
           |left join gridrru t4 on t4.gridid=t3.midgrid
           |left join $DDB.grid t5 on t5.gridid=t3.midgrid
           |group by gridids,t4.rrunum,t3.cnt,t5.lon,t5.lat
         """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/gt_quesroad_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")

      gridMap.getGridMap(sparkSession,"freqswgrid")

      sql(
        s"""
           |select "$cal_date",gridids,"freqswgrid","频繁切换路段",t4.rrunum,"0",t5.lon,t5.lat from
           |(select (t2.value)gridids,t1.gridid,(t2.midgrid)midgrid from
           |(select gridid from result.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR and etype=10  group by gridid order by gridid)t1
           |inner join griddatatable t2 on t1.gridid=t2.gridid)t3
           |left join gridrru t4 on t4.gridid=t3.midgrid
           |left join $DDB.grid t5 on t5.gridid=t3.midgrid
           |group by gridids,t4.rrunum,t5.lon,t5.lat
         """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/gt_quesroad_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")

      gridMap.getGridMap(sparkSession,"weakcovergrid")
      sql(
        s"""
           |select "$cal_date",gridids,"weakcovergrid","弱覆盖路段",t4.rrunum,t3.cnt,t5.lon,t5.lat from
           |(select (t2.value)gridids,t1.gridid,(t2.midgrid)midgrid,t2.cnt from
           |(select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR  group by gridid having avg(cellrsrp)<-110 order by gridid)t1
           |left join griddatatable t2 on t1.gridid=t2.gridid)t3
           |left join gridrru t4 on t4.gridid=t3.midgrid
           |left join $DDB.grid t5 on t5.gridid=t3.midgrid
           |group by gridids,t4.rrunum,t3.cnt,t5.lon,t5.lat
         """.stripMargin).write.mode(SaveMode.Append).csv(s"$warhouseDir/gt_quesroad_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
    }
}
