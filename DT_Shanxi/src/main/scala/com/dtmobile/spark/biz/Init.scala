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

    //todo 修改表名 gtUser 修改mcc字段

    //小区表
    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","(select region,city,freq1,cellid,cellname from ltecell) t")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("ltecell")


    //公网和专网视图
    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","gt_publicandprofess_new_cell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("gt_publicandprofess_new_cell")

    //配置表
    sparkSession.read.format("jdbc").option("url", s"$oracle")
      .option("dbtable","(select a.*, CAST( to_char(balence_userrate,'S999.999') AS float ) balence_userrate_f from gt_capacity_config a ) T")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("gt_capacity_config")


  }


}
