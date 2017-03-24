package com.dtmobile.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Analyse
  *
  * @author heyongjin
  * @create 2017/03/02 10:21
  *
  **/
trait Analyse {
  val appName: String
  val master: String
  val sourceDir : String
  val warhouseDir : String

   def init() = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    sparkSession
}

  def exec() = {
    implicit val sparkSession: SparkSession = init()
    checkParams()
    analyse
    sparkSession.stop()
  }

  def analyse(implicit sparkSession: SparkSession)

  def checkParams() = {
    //todo
  }
}
