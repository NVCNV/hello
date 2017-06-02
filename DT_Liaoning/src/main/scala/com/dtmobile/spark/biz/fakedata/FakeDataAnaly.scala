package com.dtmobile.spark.biz.fakedata

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.{Connection, DriverManager, ResultSet,Statement}

/**
  * Created by weiyaqin on 2017/5/2.
  */
class FakeDataAnaly(ANALY_DATE: String,ANALY_HOUR: String,SDB: String, DDB: String, warhouseDir: String,ORCAL: String) {

  var lPhone:List[String] = List()
  val lMaxMessageByte:Int = 1000

  def notifyFakeInfo(stmt:Statement, sSql:String):Unit = {
    if ( lPhone.nonEmpty ) {
      var i = 0
      var len = lPhone.length - 1
      for (i <- 0 to len) {
        stmt.execute(s"insert into smsserver_out(recipient,text,create_date) values ('" +lPhone(i) + s"','$sSql',sysdate)")
      }
    }
  }

  def notifyFakeInfos(implicit sparkSession: SparkSession):Unit = {
    import sparkSession.sql
    sql(s"use $DDB")

    //获取电话号码信息
    lPhone = sql(s"select callnumber from tb_fake_telephone").rdd.map(row=>s"${row(0)}").collect().toList

    val rddFromSql = sql(
      s"""
         | select tb.fakeid,tb.starttime,tb.endtime,tb.freq1,tb.pci,tb.cellnum from
         | (
         | select fakeid,starttime,endtime,freq1,pci,mt,cellnum,rank() over(partition by freq1,pci order by mt desc) as rank1 from
         | (
         | select t.*,unix_timestamp(starttime,'yyyy-MM-dd hh:mm:ss') as mt from tb_fake_data t
         | where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR"
         | ) tc
         | ) tb where tb.rank1 = 1 order by tb.mt desc, tb.cellnum desc
      """.stripMargin)

      val user = "scott"
      val password = "tiger"
      val conn_str = "jdbc:oracle:thin:@" + ORCAL
      Class.forName("oracle.jdbc.OracleDriver")
      val conn = DriverManager.getConnection(conn_str, user, password)

    try {
      val stmt = conn.createStatement()

      var sWhole = s"可疑基站:\n"
      var sItem = ""
      var iLen = sWhole.getBytes().length
      val it = rddFromSql.rdd.map(row => s"序号:${row(0)},时间:${row(1)},频点:${row(3)},PCI:${row(4)},影响小区数:${row(5)}").collect().iterator
      while (it.hasNext) {
        sItem = it.next()
        if ((sItem.getBytes().length + iLen) < lMaxMessageByte) {
          sWhole = sWhole + sItem + s"\n"
          iLen = sWhole.getBytes().length
        }
        else {
          notifyFakeInfo(stmt, sWhole)

          sWhole = s"可疑基站:\n" + sItem + s"\n"
          iLen = sWhole.getBytes().length
        }
      }
      if (iLen > 20) {
        //说明还有有效信息需要发送
        notifyFakeInfo(stmt, sWhole)
      }

      stmt.execute("truncate table tb_fake_data")
      stmt.execute("truncate table tb_fake_effcell")
    }catch {
      case _ : Exception => println("===>")
    }
    finally {
      conn.close()
    }
  }

  def analyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql

    sparkSession.read.format("jdbc").option("url", s"jdbc:oracle:thin:@$ORCAL")
      .option("dbtable", "ltecell")
      .option("user", "scott")
      .option("password", "tiger")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load().createOrReplaceTempView("ltecell")

    sql(s"use $DDB")

    sql(s"""alter table tb_fake_data_temp drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""")
    sql(s"""alter table tb_fake_data_temp add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/tb_fake_data_temp/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
/*
    sql(
      s"""
         | select t.starttime,t.endtime,t.meatime,t.enbid,t.cellid,t.gridcenterlongitude,t.gridcenterlatitude,t.kpi1,t.kpi2,t.kpi11,t.kpi12,
         | t.mmeues1apid,t.mmegroupid,t.mmecode,floor(c.distance)
         | from $SDB.lte_mro_source t left join tb_cell_distance c on t.cellid = c.cellid and t.kpi12 = c.pci and t.kpi11 = c.freq1
         | where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR"
         | and t.mrname = 'MR.LteScRSRP' and t.kpi2 - t.kpi1 >= 0 and t.kpi2 >= 0 and  t.eventtype <> 'PERIOD'
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_fake_data_temp/dt=$ANALY_DATE/h=$ANALY_HOUR")
*/
///*
    sql(
      s"""
         | select t.starttime,t.endtime,t.meatime,t.enbid,t.cellid,t.gridcenterlongitude,t.gridcenterlatitude,t.kpi1,t.kpi2,t.kpi11,t.kpi12,
         | t.mmeues1apid,t.mmegroupid,t.mmecode,floor(c.distance)
         | from
         | (select a.* from $SDB.lte_mro_source a left join lte2lteadj_pci b on a.cellid=b.cellid and a.kpi12=b.adjpci
         | and a.kpi11=b.adjfreq1 where b.adjcellid is null and a.dt="$ANALY_DATE" and a.h="$ANALY_HOUR" and a.mrname = 'MR.LteScRSRP'
         | and a.kpi2 - a.kpi1 >= 0 and a.kpi2 >= 0 and a.eventtype <> 'PERIOD') t
         | left join tb_cell_distance c on t.cellid = c.cellid and t.kpi12 = c.pci and t.kpi11 = c.freq1
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_fake_data_temp/dt=$ANALY_DATE/h=$ANALY_HOUR")
//*/
    //    insert into tb_fake_data (starttime,endtime,freq1,pci,cellnum,sessionnum,lon,lat,numcount)
    sql(s"""alter table tb_fake_data drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""")
    sql(s"""alter table tb_fake_data add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/tb_fake_data/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select ROW_NUMBER() OVER(partition by id order by starttime,freq1,pci) as fakeid,
         | starttime,endtime,freq1,pci,cellnum,sessionnum,lon,lat,numcount from (
         | select 1 as id,
         | starttime,endtime, kpi11 freq1,kpi12 pci,count(distinct t.cellid) cellnum,count(distinct mmeUEs1apid) sessionnum,
         | avg(c.longitude) lon,
         | avg(c.latitude) lat ,count(*) numcount
         | from tb_fake_data_temp t left join ltecell c on c.cellid = t.cellid
         | where (t.adjcellid is  null or t.adjcellid > 2) and kpi11 is not null
         | and t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR"
         | group by starttime,endtime, kpi11, kpi12  having count(*) >=150 and count(distinct t.cellid) >=10 )
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_fake_data/dt=$ANALY_DATE/h=$ANALY_HOUR")

    //insert  /*+append*/   into tb_fake_effcell
    sql(s"""alter table tb_fake_effcell drop if exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)""")
    sql(s"""alter table tb_fake_effcell add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)
    LOCATION 'hdfs://dtcluster/$warhouseDir/tb_fake_effcell/dt=$ANALY_DATE/h=$ANALY_HOUR'""")
    sql(
      s"""
         | select tb.starttime, tb.endtime,tb.pci,tb.freq1,tb.cellid,tb.sessionnum,tb.kpi1,tb.kpi2 from
         | (
         | select starttime,endtime, kpi11 freq1,kpi12 pci,cellid,count(distinct mmeUEs1apid) sessionnum,avg(kpi1) kpi1,avg(kpi2) kpi2,
         | count(*) numcount from tb_fake_data_temp t
         | where (t.adjcellid is null or t.adjcellid >2 ) and kpi11 is not null
         | and t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR"
         | group by starttime,endtime, kpi11, kpi12 , cellid
         | ) tb
         | right join tb_fake_data c on tb.freq1 = c.freq1 and tb.pci = c.pci and tb.starttime=c.starttime
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/tb_fake_effcell/dt=$ANALY_DATE/h=$ANALY_HOUR")

    notifyFakeInfos(sparkSession) //短信通知
  }
}
