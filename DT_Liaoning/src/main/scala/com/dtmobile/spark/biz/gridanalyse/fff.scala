package com.dtmobile.spark.biz.gridanalyse

import com.dtmobile.spark.SparkSessionSingleton
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhoudehu on 2017/5/18/0018.
  */
object fff {

  def main(args: Array[String]) {
    val conf= new SparkConf().setAppName("PCI").setMaster("local[3]");
    //    val ssc = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
    val sc= SparkSessionSingleton.getInstance(conf)


    var oracle = "jdbc:oracle:thin:@"+"172.30.4.159:1521/morpho0503"
    val CellDF = sc.read
      .format("jdbc")
      .option("url", s"$oracle")
      .option("dbtable", "(select * from grid_view) t")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver").load().createOrReplaceTempView("grid_view")

//    .write.mode(SaveMode.Overwrite).csv("D://grid")
   sc.sql("select * from grid_view").count()





  }


}
