package com.dtmobile.spark.biz.shanxikpi

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * KpiHourAnaly
  *
  * @author heyongjin
  * create 2017/03/02 10:36
  *
  **/
class KpiHourAnaly(ANALY_DATE: String, ANALY_HOUR: String, SDB: String, DDB: String, warhouseDir: String,versiononoff:Int) {
  val cal_date = ANALY_DATE.substring(0, 4) + "-" + ANALY_DATE.substring(4).substring(0,2) + "-" + ANALY_DATE.substring(6) + " " + String.valueOf(ANALY_HOUR) + ":00:00"
  var onoff=versiononoff
  if(versiononoff!=0 && versiononoff!=1){
    onoff=0
  }

  var procedurestatussuccess=1
  var procedurestatusfaile=2
  var ServiceTypeaudio=1
  var ServiceTypevideo=2
  var callsidecalling=1
  var callsedediacalled=2
  var timetr=100


  def analyse(implicit sparkSession: SparkSession): Unit = {


    imsiCellHourAnalyse(sparkSession)
    cellHourAnalyse(sparkSession)
    mrImsiHourAnalyse(sparkSession)
    mrCellHourAnalyse(sparkSession)
  }

  def imsiCellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    if(onoff==1){
      procedurestatussuccess = 0
      ServiceTypeaudio=0
      ServiceTypevideo=1
      callsidecalling=0
      callsedediacalled=1
    }

    import sparkSession.sql
    sql(s"use $DDB")
    sql(s"alter table volte_gt_user_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")

    sql(s"use $DDB")

    val uu = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		)
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswsucc,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 4 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcrebuild,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbinx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         |  0 AS srvccsuccS1
         |FROM
         |	 $DDB.TB_XDR_IFC_UU
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    val x2 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$DDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    val sv = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(SOURCEECI) cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			SVDELAY/$timetr
         |		END
         |	) srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         |  0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_SV
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	SOURCEECI
       """.stripMargin)

    val voltesip = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(sourceeci) cellid,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_MW
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	sourceeci
       """.stripMargin)

    val voltesip0 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(sourceeci) cellid,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |			callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypeaudio
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_MW
         |WHERE
         |	callside = $callsidecalling
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	sourceeci
       """.stripMargin)

    val voltesip1 = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	(desteci) cellid,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |			callduration
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypeaudio
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypeaudio
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = $procedurestatussuccess THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_mw
         |WHERE
         |	callside = $callsedediacalled
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	desteci
       """.stripMargin)

    val s1mme = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srqatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) AS srqsucc,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tauatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 20
         |		AND Keyword1 = 0
         |		AND RequestCause <> 65535
         |		AND RequestCause NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wirelessdrop,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 18
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wireless,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 21
         |		AND BEARER0REQUESTCAUSE <> 65535
         |		AND BEARER1REQUESTCAUSE <> 65535
         |		AND BEARER2REQUESTCAUSE <> 65535
         |		AND BEARER3REQUESTCAUSE <> 65535
         |		AND BEARER4REQUESTCAUSE <> 65535
         |		AND BEARER5REQUESTCAUSE <> 65535
         |		AND BEARER6REQUESTCAUSE <> 65535
         |		AND BEARER7REQUESTCAUSE <> 65535
         |		AND BEARER8REQUESTCAUSE <> 65535
         |		AND BEARER9REQUESTCAUSE <> 65535
         |		AND BEARER10REQUESTCAUSE <> 65535
         |		AND BEARER11REQUESTCAUSE <> 65535
         |		AND BEARER12REQUESTCAUSE <> 65535
         |		AND BEARER13REQUESTCAUSE <> 65535
         |		AND BEARER14REQUESTCAUSE <> 65535
         |		AND BEARER15REQUESTCAUSE <> 65535
         |		AND BEARER0REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER1REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER2REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER3REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER4REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER5REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER6REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER7REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER8REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER9REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER10REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER11REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER12REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER13REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER14REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER15REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |		AND keyword1 = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabs1swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) attachx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) attachy,
         |	0 AS voltesucc,
         |  sum(CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |    AND keyword1=3 and PROCEDURESTATUS=0 THEN
         |			1
         |		ELSE
         |			0
         |		END)srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_S1MME T
         |WHERE
         |	T.dt = $ANALY_DATE
         |AND T.h = $ANALY_HOUR
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    val s1mmeHandOver = sql(
      s"""
         |SELECT
         |	imsi,
         |	msisdn,
         |	CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	count(1) AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	count(1) AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	(
         |		SELECT DISTINCT
         |			S1MME_1.*
         |		FROM
         |			(
         |				SELECT
         |					*
         |				FROM
         |					$SDB.TB_XDR_IFC_S1MME
         |				WHERE
         |					dt = $ANALY_DATE
         |				AND h = $ANALY_HOUR
         |				AND PROCEDURETYPE = 16
         |				AND keyword1 = 1
         |				AND PROCEDURESTATUS = 0
         |				AND IMSI IS NOT NULL
         |			) S1MME_1
         |		LEFT JOIN (
         |			SELECT
         |				*
         |			FROM
         |				$SDB.TB_XDR_IFC_S1MME
         |			WHERE
         |				dt = $ANALY_DATE
         |			AND h = $ANALY_HOUR
         |			AND PROCEDURETYPE = 20
         |			AND requestcause = 2
         |			AND IMSI IS NOT NULL
         |		) S1MME_2 ON S1MME_1.IMSI = S1MME_2.IMSI
         |		AND S1MME_1.CELLID = S1MME_2.CELLID
         |		WHERE
         |			S1MME_2.PROCEDURESTARTTIME BETWEEN S1MME_1.PROCEDURESTARTTIME
         |		AND S1MME_1.PROCEDURESTARTTIME + 6 * 1000
         |	) a
         |GROUP BY
         |	imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin)

    uu.union(x2).union(sv).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	imsi,
         |  '',
         |	msisdn,
         |	CELLID,
         | '$cal_date',
         |	sum(voltemcsucc),
         |	sum(voltemcatt),
         |	sum(voltevdsucc),
         |	sum(voltevdatt),
         |	sum(voltetime),
         |	sum(voltemctime),
         |	sum(voltemctimey),
         |  sum(voltevdtime),
         |  sum(voltevdtimey),
         |	sum(voltemchandover),
         |	sum(volteanswer),
         |	sum(voltevdhandover),
         |	sum(voltevdanswer),
         |	sum(srvccsucc),
         |	sum(srvccatt),
         |	sum(srvcctime),
         |	sum(lteswsucc),
         |	sum(lteswatt),
         |	sum(srqatt),
         |	sum(srqsucc),
         |	sum(tauatt),
         |	sum(tausucc),
         |	sum(rrcrebuild),
         |	sum(rrcsucc),
         |	sum(rrcreq),
         |	sum(imsiregatt),
         |	sum(imsiregsucc),
         |	sum(wirelessdrop),
         |	sum(wireless),
         |	sum(eabdrop),
         |	sum(eab),
         |	sum(eabs1swx),
         |	sum(eabs1swy),
         |	sum(s1tox2swx),
         |	sum(s1tox2swy),
         |	sum(enbx2swx),
         |	sum(enbx2swy),
         |	sum(uuenbswx),
         |	sum(uuenbswy),
         |	sum(uuenbinx),
         |	sum(uuenbiny),
         |	sum(swx),
         |	sum(swy),
         |	sum(attachx),
         |	sum(attachy),
         |	sum(voltesucc),
         |  sum(srvccsuccS1)
         |from temp_kpi
         |group by imsi,
         |	msisdn,
         |	CELLID
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_user_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def cellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    if(onoff==1){
      procedurestatussuccess = 0
      ServiceTypeaudio=0
      ServiceTypevideo=1
      callsidecalling=0
      callsedediacalled=1
    }
    sql(s"use $DDB")
    sql(s"alter table volte_gt_cell_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    val uu = sql(
      s"""
         |SELECT
         |	cellid AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		)
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswsucc,
         |	sum(
         |		CASE
         |		WHEN (
         |			proceduretype = 7
         |			OR proceduretype = 8
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 4 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcrebuild,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 8 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbswy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbinx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 7 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_UU
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID
       """.stripMargin)
    val x2 = sql(
      s"""
         |SELECT
         |	CELLID  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND (
         |			ProcedureStatus = 0
         |			OR (
         |				(
         |					ProcedureStatus BETWEEN 1
         |					AND 255
         |				)
         |				AND (
         |					failurecause != 1000
         |					OR failurecause IS NULL
         |				)
         |			)
         |		) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swx,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_X2
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID
       """.stripMargin)
    val sv = sql(
      s"""
         |SELECT
         |	(SOURCEECI)  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srvccatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND RESULT = 0 THEN
         |			SVDELAY/$timetr
         |		END
         |	) srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_SV
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	SOURCEECI
       """.stripMargin)
    val voltesip = sql(
      s"""
         |SELECT
         |	(sourceeci)  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_MW
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	sourceeci
       """.stripMargin)
    val voltesip0 = sql(
      s"""
         |SELECT
         |	(sourceeci)  AS CELLID,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND callduration <> 4294967295 THEN
         |			callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = 1
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_MW
         |WHERE
         |	callside = $callsidecalling
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	sourceeci
       """.stripMargin)
    val voltesip1 = sql(
      s"""
         |SELECT
         |	(desteci)  AS CELLID,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltemcatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdsucc,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND alertingtime <> 4294967295 THEN
         |			alertingtime/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltetime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND callduration <> 4294967295 THEN
         |			callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltemctime,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = 1
         |		AND callduration <> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltemctimey,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		callduration/$timetr
         |		ELSE
         |			0
         |		END
         |	) voltevdtime,
         | sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND interface = 14
         |		AND ServiceType = $ServiceTypevideo and callduration<> 4294967295 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) voltevdtimey,
         |	0 AS voltemchandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = 1
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) volteanswer,
         |	0 AS voltevdhandover,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND ServiceType = $ServiceTypevideo
         |		AND Answertime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregatt,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 1
         |		AND interface = 14
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	sum(
         |		CASE
         |		WHEN ProcedureType = 5
         |		AND alertingtime <> 4294967295 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_MW
         |WHERE
         |	callside = $callsedediacalled
         |AND dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	desteci
       """.stripMargin)
    val s1mme = sql(
      s"""
         |SELECT
         |	CELLID  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) srqatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 2
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) AS srqsucc,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tauatt,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 5
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 20
         |		AND Keyword1 = 0
         |		AND RequestCause <> 65535
         |		AND RequestCause NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wirelessdrop,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 18
         |		AND ProcedureStatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) wireless,
         |	sum(
         |		CASE
         |		WHEN proceduretype = 21
         |		AND BEARER0REQUESTCAUSE <> 65535
         |		AND BEARER1REQUESTCAUSE <> 65535
         |		AND BEARER2REQUESTCAUSE <> 65535
         |		AND BEARER3REQUESTCAUSE <> 65535
         |		AND BEARER4REQUESTCAUSE <> 65535
         |		AND BEARER5REQUESTCAUSE <> 65535
         |		AND BEARER6REQUESTCAUSE <> 65535
         |		AND BEARER7REQUESTCAUSE <> 65535
         |		AND BEARER8REQUESTCAUSE <> 65535
         |		AND BEARER9REQUESTCAUSE <> 65535
         |		AND BEARER10REQUESTCAUSE <> 65535
         |		AND BEARER11REQUESTCAUSE <> 65535
         |		AND BEARER12REQUESTCAUSE <> 65535
         |		AND BEARER13REQUESTCAUSE <> 65535
         |		AND BEARER14REQUESTCAUSE <> 65535
         |		AND BEARER15REQUESTCAUSE <> 65535
         |		AND BEARER0REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER1REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER2REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER3REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER4REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER5REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER6REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER7REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER8REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER9REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER10REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER11REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER12REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER13REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER14REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514)
         |		AND BEARER15REQUESTCAUSE NOT IN (2, 20, 23, 24, 28, 512, 514) THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabdrop,
         |	0 AS eab,
         |	0 AS eabs1swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |		AND keyword1 = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) eabs1swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 14 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	0 AS swx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) swy,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1
         |		AND procedurestatus = 0 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) attachx,
         |	sum(
         |		CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 1 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) attachy,
         |	0 AS voltesucc,
         | sum(CASE
         |		WHEN INTERFACE = 5
         |		AND proceduretype = 16
         |    AND keyword1=3 and PROCEDURESTATUS=0 THEN
         |			1
         |		ELSE
         |			0
         |		END)srvccsuccS1
         |FROM
         |	$SDB.TB_XDR_IFC_S1MME T
         |WHERE
         |	T.dt = $ANALY_DATE
         |AND T.h = $ANALY_HOUR
         |GROUP BY
         |	CELLID
       """.stripMargin)
    val s1mmeHandOver = sql(
      s"""
         |SELECT
         |	CELLID  AS CELLID,
         |	0 AS voltemcsucc,
         |	0 AS voltemcatt,
         |	0 AS voltevdsucc,
         |	0 AS voltevdatt,
         |	0 AS voltetime,
         |	0 AS voltemctime,
         |	0 AS voltemctimey,
         |  0 AS voltevdtime,
         |  0 AS voltevdtimey,
         |	0 AS voltemchandover,
         |	0 AS volteanswer,
         |	0 AS voltevdhandover,
         |	0 AS voltevdanswer,
         |	0 AS srvccsucc,
         |	0 AS srvccatt,
         |	0 AS srvcctime,
         |	0 AS lteswsucc,
         |	0 AS lteswatt,
         |	0 AS srqatt,
         |	0 AS srqsucc,
         |	0 AS tauatt,
         |	0 AS tausucc,
         |	0 AS rrcrebuild,
         |	0 AS rrcsucc,
         |	0 AS rrcreq,
         |	0 AS imsiregatt,
         |	0 AS imsiregsucc,
         |	0 AS wirelessdrop,
         |	0 AS wireless,
         |	0 AS eabdrop,
         |	0 AS eab,
         |	count(1) AS eabs1swx,
         |	0 AS eabs1swy,
         |	0 AS s1tox2swx,
         |	0 AS s1tox2swy,
         |	0 AS enbx2swx,
         |	0 AS enbx2swy,
         |	0 AS uuenbswx,
         |	0 AS uuenbswy,
         |	0 AS uuenbinx,
         |	0 AS uuenbiny,
         |	count(1) AS swx,
         |	0 AS swy,
         |	0 AS attachx,
         |	0 AS attachy,
         |	0 AS voltesucc,
         | 0 AS srvccsuccS1
         |FROM
         |	(
         |		SELECT DISTINCT
         |			S1MME_1.*
         |		FROM
         |			(
         |				SELECT
         |					*
         |				FROM
         |					$SDB.TB_XDR_IFC_S1MME
         |				WHERE
         |					dt = $ANALY_DATE
         |				AND h = $ANALY_HOUR
         |				AND PROCEDURETYPE = 16
         |				AND keyword1 = 1
         |				AND PROCEDURESTATUS = 0
         |				AND IMSI IS NOT NULL
         |			) S1MME_1
         |		LEFT JOIN (
         |			SELECT
         |				*
         |			FROM
         |				$SDB.TB_XDR_IFC_S1MME
         |			WHERE
         |				dt = $ANALY_DATE
         |			AND h = $ANALY_HOUR
         |			AND PROCEDURETYPE = 20
         |			AND requestcause = 2
         |			AND IMSI IS NOT NULL
         |		) S1MME_2 ON S1MME_1.IMSI = S1MME_2.IMSI
         |		AND S1MME_1.CELLID = S1MME_2.CELLID
         |		WHERE
         |			S1MME_2.PROCEDURESTARTTIME BETWEEN S1MME_1.PROCEDURESTARTTIME
         |		AND S1MME_1.PROCEDURESTARTTIME + 6 * 1000
         |	) a
         |GROUP BY
         |	CELLID
       """.stripMargin)

    uu.union(x2).union(sv).union(voltesip).union(voltesip0).union(voltesip1).union(s1mme).union(s1mmeHandOver).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |'$cal_date',
         |	CELLID,
         |	sum(voltemcsucc),
         |	sum(voltemcatt),
         |	sum(voltevdsucc),
         |	sum(voltevdatt),
         |	sum(voltetime),
         |	sum(voltemctime),
         |	sum(voltemctimey),
         |  sum(voltevdtime),
         |  sum(voltevdtimey),
         |	sum(voltemchandover),
         |	sum(volteanswer),
         |	sum(voltevdhandover),
         |	sum(voltevdanswer),
         |	sum(srvccsucc),
         |	sum(srvccatt),
         |	sum(srvcctime),
         |	sum(lteswsucc),
         |	sum(lteswatt),
         |	sum(srqatt),
         |	sum(srqsucc),
         |	sum(tauatt),
         |	sum(tausucc),
         |	sum(rrcrebuild),
         |	sum(rrcsucc),
         |	sum(rrcreq),
         |	sum(imsiregatt),
         |	sum(imsiregsucc),
         |	sum(wirelessdrop),
         |	sum(wireless),
         |	sum(eabdrop),
         |	sum(eab),
         |	sum(eabs1swx),
         |	sum(eabs1swy),
         |	sum(s1tox2swx),
         |	sum(s1tox2swy),
         |	sum(enbx2swx),
         |	sum(enbx2swy),
         |	sum(uuenbswx),
         |	sum(uuenbswy),
         |	sum(uuenbinx),
         |	sum(uuenbiny),
         |	sum(swx),
         |	sum(swy),
         |	sum(attachx),
         |	sum(attachy),
         |	sum(voltesucc),
         | sum(srvccsuccS1)
         |FROM
         |	temp_kpi
         |GROUP BY
         |	cellid
       """.stripMargin).repartition(20).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/volte_gt_cell_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }



  def shanximrCellHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    val CAL_DATE = ANALY_DATE + " " + ANALY_HOUR + "00:00"
    val mrtmp=
      s"""
         |(select t.*,g.rruid from lte_mro_source t
         |left join grid_rru g on t.gridid=g.gridid where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR")t1
       """.stripMargin

    sql(s"use $DDB")
    sql(s"alter table mr_gt_cell_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |SELECT
         |	cellid,
         |	xdrid,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			KPI1 - 141
         |		ELSE
         |			0
         |		END
         |	) avgrsrpx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) commy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI3 IS NOT NULL THEN
         |			KPI3 * 0.5 - 20
         |		ELSE
         |			0
         |		END
         |	) avgrsrqx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP' AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) ltecoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) <=- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) weakcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratey,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI1 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI2 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI3 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI4 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI5 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI6 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI7 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI8 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI9 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI10 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststroy,
         |	MAX(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			NULL
         |		END
         |	) upsigrateavgmax,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			46 - KPI6
         |		ELSE
         |			0
         |		END
         |	) uebootx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uebooty,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6
         |		AND KPI10 = KPI12 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststrox,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststroy
         |FROM
         |	$SDB.LTE_MRO_SOURCE
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID,
         |	xdrid
       """.stripMargin).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	cellid,
         | '$cal_date',
         | '' dir_state,
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(
         |		CASE
         |		WHEN overlapcoverratey > 3 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(overlapcoverratey),
         |	max(
         |		greatest(
         |			updiststroxvalue1,
         |			updiststroxvalue2,
         |			updiststroxvalue3,
         |			updiststroxvalue4,
         |			updiststroxvalue5,
         |			updiststroxvalue6,
         |			updiststroxvalue7,
         |			updiststroxvalue8,
         |			updiststroxvalue9,
         |			updiststroxvalue10
         |		)
         |	) updiststromax,
         |	sum(
         |		updiststroxvalue1 + updiststroxvalue2 + updiststroxvalue3 + updiststroxvalue4 + updiststroxvalue5 + updiststroxvalue6 + updiststroxvalue7 + updiststroxvalue8 + updiststroxvalue9 + updiststroxvalue10
         |	) updiststrox,
         |	sum(
         |  case when (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) is not null then
         |  (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) else 0 end
         | ) updiststroy,
         |	MAX(upsigrateavgmax),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |	sum(uebootx),
         |	sum(uebooty),
         |	sum(model3diststrox) model3diststrox,
         |	sum(model3diststroy) model3diststroy
         |FROM
         |	temp_kpi
         |GROUP BY
         |	cellid
       """.stripMargin).repartition(20).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_cell_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def shanximrImsigridHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    val mrtmp=
      s"""
         |(select t.*,g.rruid from lte_mro_source t
         |left join grid_rru g on t.gridid=g.gridid where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR")t1
       """.stripMargin

    sql(s"use $DDB")
    sql(s"alter table mr_gt_user_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |SELECT
         |	imsi,msisdn,cellid,rruid,gridid,(eupordown)dir_state,xdrid,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			KPI1 - 141
         |		ELSE
         |			0
         |		END
         |	) avgrsrpx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) commy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI3 IS NOT NULL THEN
         |			KPI3 * 0.5 - 20
         |		ELSE
         |			NULL
         |		END
         |	) avgrsrqx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP' AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) ltecoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) <=- 110 THEN
         |		1
         |		ELSE
         |			0
         |		END
         |	) weakcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratey,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI1 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI2 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI3 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI4 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI5 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI6 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI7 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI8 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI9 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI10 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststroy,
         |	MAX(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			NULL
         |		END
         |	) upsigrateavgmax,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			46 - KPI6
         |		ELSE
         |			0
         |		END
         |	) uebootx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uebooty,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6
         |		AND KPI10 = KPI12 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststrox,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststroy
         |FROM
         |	$mrtmp
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	imsi,msisdn,cellid,rruid,gridid,dir_state,xdrid
       """.stripMargin).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	imsi,
         | '',
         |	msisdn,
         |  cellid,
         |  rruid,
         |  gridid,
         | '$cal_date',
         |  dir_state,
         | '' elong,
         | '' elat,
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(
         |		CASE
         |		WHEN overlapcoverratex > 3 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(overlapcoverratey),
         |	max(
         |		greatest(
         |			updiststroxvalue1,
         |			updiststroxvalue2,
         |			updiststroxvalue3,
         |			updiststroxvalue4,
         |			updiststroxvalue5,
         |			updiststroxvalue6,
         |			updiststroxvalue7,
         |			updiststroxvalue8,
         |			updiststroxvalue9,
         |			updiststroxvalue10
         |		)
         |	) updiststromax,
         |	sum(
         |		updiststroxvalue1 + updiststroxvalue2 + updiststroxvalue3 + updiststroxvalue4 + updiststroxvalue5 + updiststroxvalue6 + updiststroxvalue7 + updiststroxvalue8 + updiststroxvalue9 + updiststroxvalue10
         |	) updiststrox,
         |	sum(
         |  case when (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) is not null then
         |  (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) else 0 end
         | ) updiststroy,
         |	MAX(upsigrateavgmax),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |	sum(uebootx),
         |	sum(uebooty),
         |	sum(model3diststrox) model3diststrox,
         |	sum(model3diststroy) model3diststroy
         |FROM
         |	temp_kpi
         |GROUP BY
         |	imsi,
         |	msisdn,
         |  cellid,
         |  rruid,
         |  gridid,
         |  dir_state
       """.stripMargin).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_user_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }

  def shanximrgridHourAnalyse(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.sql
    val CAL_DATE = ANALY_DATE + " " + ANALY_HOUR + "00:00"
    val mrtmp=
      s"""
         |(select t.*,g.rruid from lte_mro_source t
         |left join grid_rru g on t.gridid=g.gridid where t.dt="$ANALY_DATE" and t.h="$ANALY_HOUR")t1
       """.stripMargin

    sql(s"use $DDB")
    sql(s"alter table mr_gt_grid_ana_base60 add if not exists partition(dt=$ANALY_DATE,h=$ANALY_HOUR)")
    sql(
      s"""
         |SELECT
         |	cellid,rruid,gridid,(eupordown)dir_state,xdrid,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			KPI1 - 141
         |		ELSE
         |			0
         |		END
         |	) avgrsrpx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) commy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI3 IS NOT NULL THEN
         |			KPI3 * 0.5 - 20
         |		ELSE
         |			0
         |		END
         |	) avgrsrqx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP' AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) ltecoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND (KPI1 - 141) <=- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) weakcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND VID = 1
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratey,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI1 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI2 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI3 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI4 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI5 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI6 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI7 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI8 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI9 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			KPI10 * 0.1 - 126.1
         |		ELSE
         |			0
         |		END
         |	) updiststroxvalue10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox1,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox2,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI3 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox3,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI4 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox4,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI5 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox5,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI6 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox6,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI7 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox7,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI8 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox8,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI9 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox9,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI10 IS NOT NULL
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststrox10,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRIP0'
         |		AND KPI2 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) updiststroy,
         |	MAX(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			NULL
         |		END
         |	) upsigrateavgmax,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			KPI8 - 11
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI8 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) upsigrateavgy,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			46 - KPI6
         |		ELSE
         |			0
         |		END
         |	) uebootx,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI6 IS NOT NULL THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) uebooty,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6
         |		AND KPI10 = KPI12 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststrox,
         |	sum(
         |		CASE
         |		WHEN MRNAME = 'MR.LteScRSRP'
         |		AND KPI1 IS NOT NULL
         |		AND KPI2 IS NOT NULL
         |		AND (kpi1 - 141) >- 110
         |		AND abs(KPI1 - KPI2) < 6 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) model3diststroy
         |FROM
         |	$mrtmp
         |WHERE
         |	dt = $ANALY_DATE
         |AND h = $ANALY_HOUR
         |GROUP BY
         |	CELLID,rruid,gridid,eupordown,xdrid
       """.stripMargin).createOrReplaceTempView("temp_kpi")
    sql(
      s"""
         |SELECT
         |	cellid,rruid,gridid,$CAL_DATE,dir_state
         |	sum(avgrsrpx),
         |	sum(commy),
         |	sum(avgrsrqx),
         |	sum(ltecoverratex),
         |	sum(weakcoverratex),
         |	sum(
         |		CASE
         |		WHEN overlapcoverratey > 3 THEN
         |			1
         |		ELSE
         |			0
         |		END
         |	) overlapcoverratex,
         |	sum(overlapcoverratey),
         |	max(
         |		greatest(
         |			updiststroxvalue1,
         |			updiststroxvalue2,
         |			updiststroxvalue3,
         |			updiststroxvalue4,
         |			updiststroxvalue5,
         |			updiststroxvalue6,
         |			updiststroxvalue7,
         |			updiststroxvalue8,
         |			updiststroxvalue9,
         |			updiststroxvalue10
         |		)
         |	) updiststromax,
         |	sum(
         |		updiststroxvalue1 + updiststroxvalue2 + updiststroxvalue3 + updiststroxvalue4 + updiststroxvalue5 + updiststroxvalue6 + updiststroxvalue7 + updiststroxvalue8 + updiststroxvalue9 + updiststroxvalue10
         |	) updiststrox,
         |	sum(
         |  case when (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) is not null then
         |  (updiststrox1 + updiststrox2 + updiststrox3 + updiststrox4 + updiststrox5 + updiststrox6 + updiststrox7 + updiststrox8 + updiststrox9 + updiststrox10) else 0 end
         | ) updiststroy,
         |	MAX(upsigrateavgmax),
         |	sum(upsigrateavgx),
         |	sum(upsigrateavgy),
         |	sum(uebootx),
         |	sum(uebooty),
         |	sum(model3diststrox) model3diststrox,
         |	sum(model3diststroy) model3diststroy
         |FROM
         |	temp_kpi
         |GROUP BY
         |	cellid,rruid,gridid,dir_state
       """.stripMargin).repartition(20).write.mode(SaveMode.Overwrite).csv(s"$warhouseDir/mr_gt_grid_ana_base60/dt=$ANALY_DATE/h=$ANALY_HOUR")
  }
}

