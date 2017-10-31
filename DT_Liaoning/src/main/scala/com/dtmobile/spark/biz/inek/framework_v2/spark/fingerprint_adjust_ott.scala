package com.dtmobile.spark.biz.inek.framework_v2.spark

import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
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
    val day = args(0)
    val maxheight = args(1).toInt
    val deltaheight = args(2).toInt

    val filePath = "config.properties"
    val props = new Properties()
    props.load(new FileInputStream(filePath))

    val angleGridCount = props.getProperty("AngleGridCount").toInt
    val distanceGridCount = props.getProperty("DistanceGridCount").toInt
    val distanceGridStep = props.getProperty("DistanceGridStep").toInt

    val conf = new SparkConf().setAppName("fingerprint_adjust_ott")
      .set("spark.akka.timeout", "10000")
      .set("spark.network.timeout", "10000")
      .set("spark.akka.askTimeout", "10000")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    //提取校准数据OTT
    val ott_adjust = hiveContext.sql("select longitude, latitude, grid_longitude, grid_latitude, gridx, gridy, ltescrsrp, gridid, ott_grid_result.objectid, " +
      " base_parameter_cell.longitude as cell_longitude, base_parameter_cell.latitude as cell_latitude, base_parameter_cell.CellHorizonAngleToEast" +
      " ltencobjectid, ltencrsrp, c2.longitude as ncell_longitude, c2.latitude as ncell_latitude, c2.nCellHorizonAngleToEast" +
      " from ott_grid_result inner join base_parameter_cell on ott_grid_result.objectid = base_parameter_cell.objectid " +
      " inner join base_parameter_cell c2 on ott_grid_result.ltencobjectid = base_parameter_cell.objectid")
      .withColumn("calibrategridid", udfFunctions.getCalibrateGridId($"longitude", $"latitude", $"cell_longitude", $"cell_latitude", $"CellHorizonAngleToEast", lit(angleGridCount), lit(distanceGridCount), lit(distanceGridStep)))
      .withColumn("ncalibrategridid", udfFunctions.getCalibrateGridId($"longitude", $"latitude", $"ncell_longitude", $"ncell_latitude", $"nCellHorizonAngleToEast", lit(angleGridCount), lit(distanceGridCount), lit(distanceGridStep)))

    ott_adjust.registerTempTable("ott_adjust")
    val ott_adjust_outdoor = hiveContext.sql("select ott_adjust.* from ott_adjust left join gridmappingbuilding g on ott_adjust.gridid = g.gridid where g.gridid is null")
    val ott_adjust_indoor = hiveContext.sql("select ott_adjust.* from ott_adjust left join gridmappingbuilding g on ott_adjust.gridid = g.gridid where g.gridid is not null")

    ott_adjust_outdoor.registerTempTable("ott_adjust_outdoor")
    ott_adjust_indoor.registerTempTable("ott_adjust_indoor")

    val measure_avgrsrp = hiveContext.sql("select calibrategridid, avg(ltescrsrp), objectid as rsrp from ott_adjust_outdoor group by calibrategridid, objectid")
    val simulate_avgrsrp = hiveContext.sql("select calibrategridid, avg(ltescrsrp), objectid as rsrp from finger_total0 group by calibrategridid, objectid")
    val linklossCalibrate = measure_avgrsrp.join(simulate_avgrsrp, measure_avgrsrp("calibrategridid") === simulate_avgrsrp("calibrategridid") && measure_avgrsrp("objectid") === simulate_avgrsrp("objectid"))
      .select(measure_avgrsrp("objectid"), measure_avgrsrp("calibrategridid"),
        udfFunctions.parseCalibrateGridId2Angle(measure_avgrsrp("calibrategridid"), lit(distanceGridCount), lit(angleGridCount)).as("angle"),
        measure_avgrsrp("rsrp") - simulate_avgrsrp("rsrp").as("deltarsrp"),
        udfFunctions.log10(udfFunctions.parseCalibrateGridId2Distance(measure_avgrsrp("calibrategridid"), lit(distanceGridCount), lit(distanceGridStep))).as("distance"),
        simulate_avgrsrp("rsrp"),
        measure_avgrsrp("rsrp").as("measurersrp"))
      .map(s => LinkLossCalibrateModel(s.getAs[Int]("objectid"), s.getAs[Int]("calibrategridid"), s.getAs[Double]("angle"), s.getAs[Double]("deltarsrp"), s.getAs[Double]("distance"), s.getAs[Double]("measurersrp")))


    //最小二乘法进行曲线拟合，每个小区拟合一个二元多次方程。计算出小区所有校准栅格deltarsrp
    // deltarsrp入hive表，表名：LinkLossCalibrateDatabase， 字段名："CalibrateGridID"，"DeltRSRP"，"ObjectID"，"SimulateRSRP"，"MeasureRSRP"
    val cells = linklossCalibrate.map(s => s.objectid).distinct().collect()

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
      val trainData = linklossCalibrateEachCell.map(s => new LabeledPoint(s.deltarsrp, Vectors.dense(s.angle, s.distancelog10)))
      if(trainData.count() > 10) {
        val model = org.apache.spark.mllib.regression.LinearRegressionWithSGD.train(trainData, 100)

        val predictDataEachCell =  sc.makeRDD(listBuffer.toSeq).map(s => LinkLossCalibrateModel(cellId, s._1 * distanceGridCount + s._2, s._1,
          model.predict(Vectors.dense(s._1 * 360 / angleGridCount, math.log10((s._2 + 0.5) * distanceGridStep))),
            s._2, -1))

        if(predictData == null)
          predictData = predictDataEachCell
        else
          predictData = predictData.union(predictDataEachCell)
      }
    }

    predictData.toDF().registerTempTable("predict_data")

    var height = 0
    while(height <= maxheight) {
      hiveContext.sql("create table if not exists adjusted_finger_total" + height + " (gridid bigint, longitude double, latitude double, objectid int, cellname string, " +
        " gridx int, gridy int, calibrategridid int, ltencobjectid int, ncellname string, ltencrsrp double, ncalibrategridid int)")
    }

    //室外栅格校准


    val adjusted_finger_total_outdoor = hiveContext.sql("select finger_total0.gridid, finger_total0.longitude, finger_total0.latitude, finger_total0.objectid, finger_total0.cellname," +
      " case when ott_adjust_outdoor.objectid is not null then ott_adjust_outdoor.ltescrsrp when p.calibrategridid is not null then finger_total0.rsrp + p.deltarsrp else finger_total0.rsrp end rsrp, " +
      " finger_total0.gridx, finger_total0.gridy, finger_total0.calibrategridid, finger_total0.nobjectid, finger_total0.ncellname, " +
      " case when ott_adjust_outdoor.nobjectid is not null then ott_adjust_outdoor.ltencrsrp when np.calibrategridid is not null then finger_total0.nrsrp + np.deltarsrp else finger_total0.nrsrp end nrsrp, " +
      " finger_total0.ncalibrategridid " +
      " from finger_total0 left join gridmappingbuilding on finger_total0.gridid = gridmappingbuilding.gridid left join ott_adjust_outdoor on finger_total0.objectid = ott_adjust_outdoor.objectid and finger_total0.nobjectid = ott_adjust_outdoor.ltencobjectid" +
      " left join predict_data p on finger_total0.calibrategridid = p.calibrategridid and finger_total0.objectid = p.objectid" +
      " left join predict_data np on finger_total0.ncalibrategridid = np.calibrategridid and finger_total0.nobjectid = np.objectid" +
      " where gridmappingbuilding.gridid is null")

    adjusted_finger_total_outdoor.registerTempTable("adjusted_outdoor")
    hiveContext.sql("insert into table adjusted_finger_total0 select * from adjusted_outdoor")


    val ott_gridids = hiveContext.sql("select distinct gridid from ott_grid_result")
    ott_gridids.registerTempTable("ott_gridids")

    //室分室内栅格，将主服务小区替换为该室分小区，主服务小区rsrp改为[-70, -90]，原主服务小区id和rsrp添加至邻区。(排除有ott的室内栅格)
    height = 0
    while(height <= maxheight)
    {
      val adjusted_finger_total_indoor_withcell_main = hiveContext.sql("select distinct f.gridid, f.longitude, f.latitude, c.objectid, '' as cellname, rand(-70,-90) as ltescrsrp, f.gridx, f.gridy, f.calibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId ")

      val adjusted_finger_total_indoor_withcell_neighbor = hiveContext.sql("select f.gridid, f.ltencobjectid, '' as ncellname, f.ltencrsrp, f.ncalibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId " +
        " union " +
        " select distinct f.gridid, f.objectid, f.cellname as ncellname, f.ltescrsrp, f.calibrategridid " +
        " from finger_total"+height+" f inner join gridmappingbuilding g on f.gridid = g.gridid inner join CellBuildRelation c on c.BuildingId = g.BuildingId")

      adjusted_finger_total_indoor_withcell_main.registerTempTable("indoor_withcell_main"+height)
      adjusted_finger_total_indoor_withcell_neighbor.registerTempTable("indoor_withcell_neighbor"+height)

      if(height == 0)
      {
        hiveContext.sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.cellname, m.ltescrsrp, m.gridx, m.gridy, m.calibrategridid, " +
          " n.ltencobjectid, n.ncellname, n.ltencrsrp, n.ncalibrategridid " +
          " from indoor_withcell_main"+height+" m inner join indoor_withcell_neighbor"+height+" n on m.gridid = n.gridid left join ott_gridids o on m.gridid = o.gridid where o.gridid is null")

        hiveContext.sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.cellname, m.ltescrsrp, m.gridx, m.gridy, m.calibrategridid, " +
          " n.ltencobjectid, n.ncellname, n.ltencrsrp, n.ncalibrategridid " +
          " from finger_total"+height+" left join ott_gridids o on m.gridid = o.gridid where o.gridid is null")
      }
      else
      {
        hiveContext.sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.cellname, m.ltescrsrp, m.gridx, m.gridy, m.calibrategridid, " +
          " n.ltencobjectid, n.ncellname, n.ltencrsrp, n.ncalibrategridid " +
          " from indoor_withcell_main"+height+" m inner join indoor_withcell_neighbor"+height+" n on m.gridid = n.gridid")

        hiveContext.sql("insert into table adjusted_finger_total" + height +
          " select m.gridid, m.longitude, m.latitude, m.objectid, m.cellname, m.ltescrsrp, m.gridx, m.gridy, m.calibrategridid, " +
          " n.ltencobjectid, n.ncellname, n.ltencrsrp, n.ncalibrategridid " +
          " from finger_total"+height+"")
      }

      height = height + deltaheight
    }

    // 无室分室内栅格，直接用ott/扫频/路测替换，先实现OTT
    // select longitude, latitude, ltescrsrp, gridid, objectid, " +
    // cell_longitude, cell_latitude, CellHorizonAngleToEast" +
    // ltencobjectid, ltencrsrp, ncell_longitude, ncell_latitude, nCellHorizonAngleToEast (
    // 栅格经纬度,gridx,gridy,
    val ott_adjust_indoor_maincell = hiveContext.sql("select gridid, grid_longitude, grid_latitude, objectid, avg(ltescrsrp) ltescrsrp, gridx, gridy, first(calibrategridid) as calibrategridid from ott_adjust_indoor group by gridid, objectid, gridx, gridy ")
    val ott_adjust_indoor_neighborcell = hiveContext.sql("select gridid, ltencobjectid, avg(ltencrsrp) ltencrsrp, first(ncalibrategridid) as ncalibrategridid from ott_adjust_indoor group by gridid, ltencobjectid ")
    ott_adjust_indoor_maincell.registerTempTable("ott_adjust_indoor_maincell")
    ott_adjust_indoor_neighborcell.registerTempTable("ott_adjust_indoor_neighborcell")

    //有ott的栅格，直接用ott校准数据替换指纹信息
    val adjusted_finger_total_indoor_withoutcell = hiveContext.sql(
      "select m.gridid, m.grid_longitude, m.grid_latitude, m.objectid, '' as cellname, m.ltescrsrp, " +
      " m.gridx, m.gridy, m.calibrategridid,  n.ltencobjectid, '' as ncellname, n.ltencrsrp, n.ncalibrategridid " +
      " from ott_adjust_indoor_maincell m inner join ott_adjust_indoor_neighborcell n on m.gridid = n.gridid")

    adjusted_finger_total_indoor_withoutcell.registerTempTable("adjusted_ott_indoor")
    hiveContext.sql("insert into table adjusted_finger_total0 select * from adjusted_ott_indoor")

  }

  case class LinkLossCalibrateModel(objectid:Int, calibrategridid: Int, angle:Double, deltarsrp:Double, distancelog10:Double, measurersrp:Double)

}