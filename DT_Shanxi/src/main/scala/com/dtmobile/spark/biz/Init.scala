package com.dtmobile.spark.biz

import org.apache.spark.sql.SparkSession

/**
  * Created by zhoudehu on 2017/5/25/0025.
  */
class Init(ORCAL: String ){
  var oracle = "jdbc:oracle:thin:@"+ORCAL
  def analyse(implicit sparkSession: SparkSession): Unit ={
    initTable(sparkSession)
  }



  def initTable(implicit sparkSession: SparkSession): Unit ={

    //todo 修改表名
    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","(select region,city, mcc,cellid from ltecell) t")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("ltecell")

    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","gt_capacity_config")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("gt_capacity_config")


  }


}
