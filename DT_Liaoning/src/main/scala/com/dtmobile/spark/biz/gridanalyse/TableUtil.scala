package com.dtmobile.spark.biz.gridanalyse


import java.sql.DriverManager

import com.dtmobile.spark.SparkSessionSingleton
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhoudehu on 2017/5/11/0011.
  */
class TableUtil(url:String, dataBase:String, conf:SparkConf) {
  var oracleUrl = "jdbc:oracle:thin:@"+url+":1521/"+dataBase
  val sc = SparkSessionSingleton.getInstance(conf)
  val LTECell = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","LTECell")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("LTECell")


  val Lte2lteadj = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","lte2lteadj")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("lte2lteadj")

  val Ltecover_degree_condition = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltecover_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("ltecover_degree_condition")


 val ltedisturb_degree_condition = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltedisturb_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("ltedisturb_degree_condition")

  val ltemrsegment_config = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltemrsegment_config")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("ltemrsegment_config")

  val ltemrsegment_type = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltemrsegment_type")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("ltemrsegment_type")

  val lteadjcell_degree_condition = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","lteadjcell_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("lteadjcell_degree_condition")

  val ltepci_degree_condition = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltepci_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load().cache().createOrReplaceTempView("ltepci_degree_condition")


  val fill_tenbid_tcellid = sc.read.format("jdbc").option("url", oracleUrl)
    .option("dbtable","ltepci_degree_condition")
    .option("user", "scott")
    .option("password", "tiger")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  sc.sql(
    s"""
       |select cellid,freq1,pci,tcellid,tenbid,d from
       |(select cellid,freq1,pci,tcellid,tenbid,d,rank() over(partition by cellid,freq1,pci order by d ) as rank from
       |(
       |select t.cellid as cellid,a.cellid as tcellid,a.enodebid as tenbid,a.freq1,a.pci,a.latitude as alatitude,
       |(POWER(ABS(a.LATITUDE-t.latitude), 2) + POWER(ABS(a.LONGITUDE-t.longitude), 2)) as d,
       |a.longitude as alongitude,t.latitude as tlatitude,t.longitude as tlongitude
       |from LTECell t,ltecell a where t.cellid!=a.cellid and t.freq1=a.freq1 and t.pci=a.pci
       |)
       |) where rank = 1
     """.stripMargin).cache().createOrReplaceTempView("fill_tenbid_tcellid")


//  sc.sql("select * from  fill_tenbid_tcellid").show()

//  sc.sql("select * from  ltepci_degree_condition").show()
  //  sc.sql("select * from  ltepci_degree_condition").show()

}
