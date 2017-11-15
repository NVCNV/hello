package com.dtmobile.spark.biz.inek.framework_v2.spark

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


/**
 * ott校准指纹库
  * 前置条件：OTT校准数据已生成，工参已导入Hive表
 * 1. ott_grid_result
 * 2. base_parameter_cell
 */
object fingerprint_adjust_ott {
  def main(args: Array[String]) {
    //2017040508
//    val day = args(0)
//    val maxheight = args(1).toInt
//    val deltaheight = args(2).toInt

//    val filePath = "config.properties"
//    val props = new Properties()
//     props.load(new FileInputStream(filePath))

 /*   val angleGridCount = props.getProperty("AngleGridCount").toInt
    val distanceGridCount = props.getProperty("DistanceGridCount").toInt
    val distanceGridStep = props.getProperty("DistanceGridStep").toInt
    */

    val maxheight:Int = 600
    val deltaheight:Int = 5

    val angleGridCount:Int = 10
    val distanceGridCount:Int = 10
    val distanceGridStep:Int = 5


    val conf = new SparkConf().setAppName("fingerprint_adjust_ott")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.setMaster("spark://datanode01:7077")
//    conf.set("spark.driver.memory","10g")
//    conf.set("spark.testing.memory", "2147480000")

//    conf.setJars(List("E:\\gitspace\\DT_Analy\\DT_Liaoning\\target\\DT_Liaoning-1.0-SNAPSHOT.jar"))
//    conf.set("spark.executor.memory","4G")
//    conf.set("spark.executor.cores","2")
//
//      .set("spark.akka.timeout", "10000")
//      .set("spark.network.timeout", "10000")
//      .set("spark.akka.askTimeout", "10000")

    val sc = new SparkContext(conf)
//    val hiveContext = new HiveContext(sc)
    val hiveContext = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import hiveContext.implicits._
    import hiveContext.sql

    hiveContext.sql("use liaoning")

    //提取校准数据OTT
    //提取校准数据OTT
/*    val ott_adjust = hiveContext.sql("select longitude, latitude, grid_longitude, grid_latitude, gridx, gridy, ltescrsrp, gridid, ott_grid_result.objectid, " +
      " base_parameter_cell.longitude as cell_longitude, base_parameter_cell.latitude as cell_latitude, base_parameter_cell.CellHorizonAngleToEast" +
      " ltencobjectid, ltencrsrp, c2.longitude as ncell_longitude, c2.latitude as ncell_latitude, c2.nCellHorizonAngleToEast" +
      " from ott_grid_result inner join base_parameter_cell on ott_grid_result.objectid = base_parameter_cell.objectid " +
      " inner join base_parameter_cell c2 on ott_grid_result.ltencobjectid = base_parameter_cell.objectid")*/





    val ott_adjust = hiveContext.sql("select otgr.longitude,"+
      " otgr.latitude,"+
      " otgr.grid_longitude,"+
      " otgr.grid_latitude,"+
      " otgr.ltescrsrp,"+
      " otgr.gridid,"+
      " otgr.objectid,"+
      " c1.longitude as cell_longitude,"+
      " c1.latitude as cell_latitude,"+
      " c1.CellHorizonAngleToEast," +
      " otgr.ltencoid as ltencobjectid,"+
      " otgr.ltencrsrp,"+
      " c2.longitude as ncell_longitude,"+
      " c2.latitude as ncell_latitude,"+
      " c2.CellHorizonAngleToEast as nCellHorizonAngleToEast"+
      " from ott_grid_result otgr"+
      " inner join base_parameter_cell c1"+
      " on otgr.objectid = c1.objectid"+
      " inner join base_parameter_cell c2"+
      " on otgr.ltencoid = c2.objectid")
      .withColumn("calibrategridid", udfFunctions.getCalibrateGridId($"longitude", $"latitude", $"cell_longitude", $"cell_latitude", $"ltencobjectid", lit(angleGridCount), lit(distanceGridCount), lit(distanceGridStep)))
      .withColumn("ncalibrategridid", udfFunctions.getCalibrateGridId($"longitude", $"latitude", $"ncell_longitude", $"ncell_latitude", $"nCellHorizonAngleToEast", lit(angleGridCount), lit(distanceGridCount), lit(distanceGridStep)))


    ott_adjust.createOrReplaceTempView("ott_adjust")

    val ott_adjust_outdoor = sql("select ott_adjust.* from ott_adjust left join gridmappingbuilding g on ott_adjust.gridid = g.gridid where g.gridid is null")
    val ott_adjust_indoor = sql("select ott_adjust.* from ott_adjust left join gridmappingbuilding g on ott_adjust.gridid = g.gridid where g.gridid is not null")


    ott_adjust_outdoor.createOrReplaceTempView("ott_adjust_outdoor")
    ott_adjust_indoor.createOrReplaceTempView("ott_adjust_indoor")

    val measure_avgrsrp = sql("select calibrategridid, avg(ltescrsrp) as rsrp, objectid from ott_adjust_outdoor group by calibrategridid, objectid" +
                            "  union " +
                            " select ncalibrategridid, avg(ltencrsrp) as rsrp, ltencobjectid from ott_adjust_outdoor group by ncalibrategridid, ltencobjectid")
    val simulate_avgrsrp = sql("select calibrategridid, avg(rsrp) as rsrp, objectid  from finger_total0 group by calibrategridid, objectid" +
                                " union " +
                                " select ncalibrategridid, avg(n_rsrp) as rsrp, n_objectid from finger_total0 group by ncalibrategridid, n_objectid")

    val linklossCalibrate = measure_avgrsrp.join(simulate_avgrsrp, measure_avgrsrp("calibrategridid") === simulate_avgrsrp("calibrategridid") && measure_avgrsrp("objectid") === simulate_avgrsrp("objectid"))
      .select(simulate_avgrsrp("objectid"), measure_avgrsrp("calibrategridid"),
        udfFunctions.parseCalibrateGridId2Angle(measure_avgrsrp("calibrategridid"), lit(distanceGridCount), lit(angleGridCount)).as("angle"),
        (measure_avgrsrp("rsrp") - simulate_avgrsrp("rsrp")).as("deltarsrp"),
        udfFunctions.log10(udfFunctions.parseCalibrateGridId2Distance(measure_avgrsrp("calibrategridid"), lit(distanceGridCount), lit(distanceGridStep))).as("distance"),
        measure_avgrsrp("rsrp").as("measurersrp"))
      .map(s => LinkLossCalibrateModel(
        s.getAs[Int]("objectid"),
        s.getAs[Int]("calibrategridid"),
        s.getAs[Int]("angle"),
        s.getAs[Double]("deltarsrp"),
        s.getAs[Double]("distance"),
        s.getAs[Double]("measurersrp")))


    //最小二乘法进行曲线拟合，每个小区拟合一个二元多次方程。计算出小区所有校准栅格deltarsrp
    // deltarsrp入hive表，表名：LinkLossCalibrateDatabase， 字段名："CalibrateGridID"，"DeltRSRP"，"ObjectID"，"SimulateRSRP"，"MeasureRSRP"
    val cells = linklossCalibrate.groupByKey(s => s.objectid).count().filter(t => t._2 > 1).map(s => s._1).collect().toList

    val listBuffer = new ListBuffer[(Int, Int)]
    for(a <- 0 until angleGridCount)
    {
      for(d <- 0 until distanceGridCount)
      {
        listBuffer.append((a, d))
      }
    }

    var predictData:RDD[LinkLossCalibrateModel] = null


    for(cellId <- cells) {
      val linklossCalibrateEachCell = linklossCalibrate.filter(s => s.objectid == cellId)

      val trainData = linklossCalibrateEachCell.rdd.map{
        s =>  new LabeledPoint(s.deltarsrp, Vectors.dense(s.angle, s.distancelog10))
      }

      trainData.foreach(println(_))
      System.exit(1)
      //TODO : add a .rdd
      val model = org.apache.spark.mllib.regression.LinearRegressionWithSGD.train(trainData,100)

/*      val lr1 = new LinearRegression()
      val lr2 = lr1.setFeaturesCol("features").setLabelCol("Murder").setFitIntercept(true)
      val lr3 = lr2.setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val lr = lr3
      val model = lr.fit(trainData)*/

      //TODO : remove toSwq
      val predictDataEachCell =  sc.makeRDD(listBuffer).map(s => LinkLossCalibrateModel(cellId, s._1 * distanceGridCount + s._2, s._1,
        model.predict(Vectors.dense(s._1 * 360 / angleGridCount, math.log10((s._2 + 0.5) * distanceGridStep))),
        s._2, -1))

      if(predictData == null)
        predictData = predictDataEachCell
      else
        predictData = predictData.union(predictDataEachCell)
    }

    if(predictData == null) {
      return
    }

    predictData.toDF().createOrReplaceTempView("predict_data")

    var height = 0
    while(height <= maxheight) {
      hiveContext.sql("create table if not exists adjusted_finger_total" + height + " (gridid string, longitude double, latitude double, objectid int, ltescrsrp double," +
        " calibrategridid int, ltencobjectid int, ltencrsrp double, ncalibrategridid int)")
      height = height + deltaheight
    }

    //室外栅格校准
    val adjusted_finger_total_outdoor = sql("select finger_total0.gridid, finger_total0.longitude, finger_total0.latitude, finger_total0.objectid, " +
      " case when ott_adjust_outdoor.objectid is not null then ott_adjust_outdoor.ltescrsrp when p.calibrategridid is not null then finger_total0.rsrp + p.deltarsrp else finger_total0.rsrp end rsrp, " +
      " finger_total0.calibrategridid, finger_total0.n_objectid, " +
      " case when ott_adjust_outdoor.ltencobjectid is not null then ott_adjust_outdoor.ltencrsrp when np.calibrategridid is not null then finger_total0.n_rsrp + np.deltarsrp else finger_total0.n_rsrp end nrsrp, " +
      " finger_total0.ncalibrategridid " +
      " from finger_total0 left join gridmappingbuilding on finger_total0.gridid = gridmappingbuilding.gridid " +
      " left join ott_adjust_outdoor on finger_total0.objectid = ott_adjust_outdoor.objectid and finger_total0.n_objectid = ott_adjust_outdoor.ltencobjectid" +
      " left join predict_data p on finger_total0.calibrategridid = p.calibrategridid and finger_total0.objectid = p.objectid" +
      " left join predict_data np on finger_total0.ncalibrategridid = np.calibrategridid and finger_total0.n_objectid = np.objectid" +
      " where gridmappingbuilding.gridid is null")


    adjusted_finger_total_outdoor.createOrReplaceTempView("adjusted_outdoor")
    sql("insert into table adjusted_finger_total0 select * from adjusted_outdoor")

    val ott_gridids = sql("select distinct gridid from ott_grid_result")
    ott_gridids.createOrReplaceTempView("ott_gridids")

    //室分室内栅格，将主服务小区替换为该室分小区，主服务小区rsrp改为[-70, -90]，原主服务小区id和rsrp添加至邻区。(排除有ott的室内栅格)
    height = 0
    while(height <= maxheight)
    {
      val adjusted_finger_total_indoor_withcell_main = sql("select distinct f.gridid, f.longitude, f.latitude, c.objectid, rand(-70,-90) as ltescrsrp, f.calibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId ")

      val adjusted_finger_total_indoor_withcell_neighbor = sql("select f.gridid, f.ltencobjectid, f.ltencrsrp, f.ncalibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId " +
        " union " +
        " select distinct f.gridid, f.objectid, f.ltescrsrp, f.calibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId")

      adjusted_finger_total_indoor_withcell_main.registerTempTable("indoor_withcell_main"+height)
      adjusted_finger_total_indoor_withcell_neighbor.registerTempTable("indoor_withcell_neighbor"+height)

      if(height == 0)
      {
        sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.ltescrsrp,  m.calibrategridid, " +
          " n.ltencobjectid, n.ltencrsrp, n.ncalibrategridid " +
          " from indoor_withcell_main"+height+" m inner join indoor_withcell_neighbor"+height+" n on m.gridid = n.gridid left join ott_gridids o on m.gridid = o.gridid where o.gridid is null")

        sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.ltescrsrp, m.calibrategridid, " +
          " n.ltencobjectid, n.ltencrsrp, n.ncalibrategridid " +
          " from finger_total"+height+" left join ott_gridids o on m.gridid = o.gridid where o.gridid is null")
      }
      else
      {
        sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.ltescrsrp, m.calibrategridid, " +
          " n.ltencobjectid, n.ltencrsrp, n.ncalibrategridid " +
          " from indoor_withcell_main"+height+" m inner join indoor_withcell_neighbor"+height+" n on m.gridid = n.gridid")

        sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid,  m.ltescrsrp, m.calibrategridid, " +
          " n.ltencobjectid, n.ltencrsrp, n.ncalibrategridid " +
          " from finger_total"+height+"")
      }

      height = height + deltaheight
    }

    // 无室分室内栅格，直接用ott/扫频/路测替换，先实现OTT
    // select longitude, latitude, ltescrsrp, gridid, objectid, " +
    // cell_longitude, cell_latitude, CellHorizonAngleToEast" +
    // ltencobjectid, ltencrsrp, ncell_longitude, ncell_latitude, nCellHorizonAngleToEast (
    // 栅格经纬度,gridx,gridy,
    val ott_adjust_indoor_maincell = sql("select gridid, grid_longitude, grid_latitude, objectid, avg(ltescrsrp) ltescrsrp, first(calibrategridid) as calibrategridid from ott_adjust_indoor group by gridid, objectid ")
    val ott_adjust_indoor_neighborcell = sql("select gridid, ltencobjectid, avg(ltencrsrp) ltencrsrp, first(ncalibrategridid) as ncalibrategridid from ott_adjust_indoor group by gridid, ltencobjectid ")
    ott_adjust_indoor_maincell.createOrReplaceTempView("ott_adjust_indoor_maincell")
    ott_adjust_indoor_neighborcell.createOrReplaceTempView("ott_adjust_indoor_neighborcell")

    //有ott的栅格，直接用ott校准数据替换指纹信息
    val adjusted_finger_total_indoor_withoutcell = sql(
      "select m.gridid, m.grid_longitude, m.grid_latitude, m.objectid, m.ltescrsrp, " +
      " m.calibrategridid, n.ltencobjectid, n.ltencrsrp, n.ncalibrategridid " +
      " from ott_adjust_indoor_maincell m inner join ott_adjust_indoor_neighborcell n on m.gridid = n.gridid")

    adjusted_finger_total_indoor_withoutcell.createOrReplaceTempView("adjusted_ott_indoor")
    sql("insert into table adjusted_finger_total0 select * from adjusted_ott_indoor")

  }

  case class LinkLossCalibrateModel(objectid:Int, calibrategridid: Int, angle:Double, deltarsrp:Double, distancelog10:Double, measurersrp:Double)

}
