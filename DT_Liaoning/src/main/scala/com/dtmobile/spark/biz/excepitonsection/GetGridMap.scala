package com.dtmobile.spark.biz.excepitonsection


import java.util
import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
/**
  * Created by shenkaili on 2017/10/18.
  */
class GetGridMap (ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String){

  def getGridMap(sparkSession: SparkSession,questiontype:String): util.HashMap[String,String] ={
    import sparkSession.sql
    var te:java.util.List[Row] = null
    var gridMap=new util.HashMap[String,String]()
    if(questiontype.equalsIgnoreCase("dpnetgrid")){
      te=sql(
        s"""
           |select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR and gridid is not null group by gridid having sum(case when ETYPE=14 or ETYPE=15 then 1 else 0 end)>2 order by gridid
         """.stripMargin).collectAsList()
    }
    else if(questiontype.equalsIgnoreCase("highdpmcgrid")){
      te=sql(
        s"""
           |select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR and gridid is not null group by gridid having sum(case when ETYPE=5 or ETYPE=7 then 1 else 0 end)>3 order by gridid
         """.stripMargin).collectAsList()
    }
    else if(questiontype.equalsIgnoreCase("highdistgrid")){
      te=sql(
        s"""
           |select gridid from $DDB.exception_analysis where dt=$ANALY_DATE and h=$ANALY_HOUR and gridid is not null group by gridid having avg(upsinr)<3 order by gridid
         """.stripMargin).collectAsList()
    }
    else if(questiontype.equalsIgnoreCase("freqswgrid")){
      val tt:java.util.List[Row]=sql(
        s"""
           |select procedurestarttime,gridid from $DDB.exception_analysis t where etype=10 and dt=$ANALY_DATE and h=$ANALY_HOUR and gridid is not null and gridid<>0 order by procedurestarttime
       """.stripMargin).collectAsList()


      var curprocedurestarttime=0L
      var nextprocedurestarttime=0L
      var thirdprocedurestarttime=0L
      var tmp=0

      var gridtempset=new util.HashSet[String]()
      if(tt.size>3)
      {
      for( a <- 0 to (tt.size()-3)){
        curprocedurestarttime=tt.get(a).get(0).toString.trim().toLong
        nextprocedurestarttime=tt.get(a+1).get(0).toString.trim().toLong
        thirdprocedurestarttime=tt.get(a+2).get(0).toString.trim().toLong
        if(thirdprocedurestarttime-curprocedurestarttime<30000&& a!=tt.size()-3){
          if(tt.get(a).get(1)!=null){
            gridtempset.add(tt.get(a).get(1).toString)
          }
          if(tt.get(a+1).get(1)!=null){
            gridtempset.add(tt.get(a+1).get(1).toString)
          }
          if(tt.get(a+2).get(1)!=null){
            gridtempset.add(tt.get(a+2).get(1).toString)
          }
        }else if(gridtempset.size()>0 && thirdprocedurestarttime-curprocedurestarttime>30000){
          var grids=new StringBuffer()
          var gridit=gridtempset.iterator()
          var ss=0
          while(gridit.hasNext){
            grids.append(gridit.next())
            grids.append(";")
            ss=ss+1
          }
          println(grids)
          val midgrid=grids.toString.split(";")(Math.floor(ss/2).toInt)
          println(grids.length())

          for(n<-0 to (a-tmp)){
            println(tt.get(tmp+n).get(1).toString)
            gridMap.put(tt.get(tmp+n).get(1).toString,grids.substring(0,grids.length()-1)+"="+midgrid)
          }
          gridtempset.removeAll(_)
          tmp=a+1
          ss=0
        }
      }
        //====================================
      var datalist=new util.ArrayList[String]()
      var it=gridMap.entrySet().iterator()
      while (it.hasNext){
        datalist.add(it.next().toString)
      }
      val arr2 = new Array[String](datalist.size())
      for(xx<-0 to datalist.size()-1){
        arr2(xx)=datalist.get(xx)
      }
      val rdd = sparkSession.sparkContext.parallelize(arr2)
      val schemaString = "gridid,value,midgrid"
      val schema = StructType(schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true)))
      val rowRDD = rdd.map(_.toString.split("=")).map(p=>Row(p(0),p(1),p(2)))
      val peopleDataFrame = sparkSession.sqlContext.createDataFrame(rowRDD,schema)
      peopleDataFrame.createOrReplaceTempView("griddatatable")
      return gridMap
    }}
    else if(questiontype.equalsIgnoreCase("weakcovergrid")){
      te = sql(
        s"""
           |select gridid from $DDB.exception_analysis t where dt=$ANALY_DATE and h=$ANALY_HOUR and gridid is not null group by gridid having avg(cellrsrp)<-110 order by gridid
       """.stripMargin).collectAsList()//得到RSRP低于-110的栅格
    }


    val gridSet = new util.HashSet[Integer]
    var a=0
    var curGridId=0
    var nextGridId=0
    for( a <- 0 to te.size()-2){
      if(te.get(a)!=null){
      curGridId= te.get(a).get(0).toString().toInt
      nextGridId=te.get(a+1).get(0).toString().toInt
      if(nextGridId-curGridId==1){
        gridSet.add(nextGridId)
        gridSet.add(curGridId)
      }
      }}

    val griddata=new util.ArrayList[Integer](gridSet)
    Collections.sort(griddata)
    var tmp=0
    var i=0
    val gridsb=new StringBuffer()
    val midgrid=new String()
    for(a<-0 to (griddata.size()-2)){
      if (griddata.get(a+1)-griddata.get(a)==1 && a!=griddata.size()-2){
        i=i+1
      }
      else{
        var y=0
        var cnt=0
        //===获取路段对应的异常事件次数================
        val sqlbuild=new StringBuffer()
        for(y <- 0 to i){
          gridsb.append(griddata.get(tmp+y).toString)
          gridsb.append(";")
        }
          cnt=i+1
        println(cnt)
        println("=========ssssss==================")
        //===============================================
        println(gridsb)
        for(y <- 0 to i){
         val midgrid=gridsb.toString.split(";")(Math.floor(i/2).toInt)
          gridMap.put(griddata.get(tmp+y).toString,gridsb.substring(0,gridsb.length()-1)+"="+midgrid+"="+cnt)//得到GRID对应的连续gridid
        }
        gridsb.delete(0,gridsb.length())
        i=0
        tmp=a+1
      }
    }

    //=====将map转成RDD===
    var datalist=new util.ArrayList[String]()
    var it=gridMap.entrySet().iterator()
    while (it.hasNext){
      datalist.add(it.next().toString)
    }
    val arr2 = new Array[String](datalist.size())
    for(xx<-0 to datalist.size()-1){
      arr2(xx)=datalist.get(xx)
    }
    val rdd = sparkSession.sparkContext.parallelize(arr2)
    val schemaString = "gridid,value,midgrid,cnt"
    val schema = StructType(schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true)))
    val rowRDD = rdd.map(_.toString.split("=")).map(p=>Row(p(0),p(1),p(2),p(3)))
    val peopleDataFrame = sparkSession.sqlContext.createDataFrame(rowRDD,schema)
    peopleDataFrame.createOrReplaceTempView("griddatatable")
//    sql(
//      s"""
//         |select * from griddatatable
//       """.stripMargin).show(10)
    gridMap
  }
}
