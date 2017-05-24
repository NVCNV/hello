#!/bin/bash
#明细数据库
DDLDB=$1
DB_PATH=$2
#缓存数据库
DCLDB=$3
DCL_PATH=$4
export NLS_LANG="AMERICAN_AMERICA.UTF8"
#HQL
hive<<EOF
CREATE DATABASE IF NOT EXISTS ${DDLDB};
USE ${DDLDB};
DROP TABLE IF EXISTS tb_xdr_ifc_gxrx;
CREATE EXTERNAL TABLE tb_xdr_ifc_gxrx(
  length bigint, 
  city string, 
  interface int, 
  xdrid string, 
  rat int, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype bigint, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  icid string, 
  originrealm string, 
  destinationrealm string, 
  originhost string, 
  destinationhost string, 
  sgsnsgwsigip string, 
  afappid string, 
  ccrequesttype bigint, 
  rxrequesttype bigint, 
  mediatype bigint, 
  abortcause bigint, 
  resultcode bigint, 
  experimentalresultcode bigint, 
  sessionreleasecause bigint, 
  rangetime string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
LOCATION
  '/${DB_PATH}/volte_rx';
  
DROP TABLE IF EXISTS tb_xdr_ifc_http;  
CREATE EXTERNAL TABLE tb_xdr_ifc_http(
  length bigint, 
  city bigint, 
  interface bigint, 
  xdrid bigint, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  machineipaddtype bigint, 
  sgwipaddr string, 
  enbipaddr string, 
  sgwport bigint, 
  enbport bigint, 
  enbgtpteid bigint, 
  sgwgtpteid bigint, 
  tac bigint, 
  ecgi bigint, 
  apn string, 
  apptypecode bigint, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  protocoltype bigint, 
  apptype bigint, 
  appsubtype bigint, 
  appcontent bigint, 
  appstatus bigint, 
  useripv4 string, 
  useripv6 string, 
  userport string, 
  l4protocal bigint, 
  appserveripipv4 string, 
  appserveripipv6 string, 
  appserverport string, 
  uldata bigint, 
  dldata bigint, 
  ulippacket bigint, 
  dlippacket bigint, 
  ultcppacketor bigint, 
  dltcppacketor bigint, 
  ultcppacketre bigint, 
  dltcppacketre bigint, 
  tcpestabrede bigint, 
  tcpestabdeconf bigint, 
  ulipfragpackets bigint, 
  dlipfragpackets bigint, 
  tcpfirstrede bigint, 
  tcpfirstconf bigint, 
  winsize bigint, 
  msssize bigint, 
  tcpattnum bigint, 
  tcplinkstatus bigint, 
  sessionflag bigint, 
  httpversion string, 
  transactiontype string, 
  httpstate string, 
  httpfirstrede bigint, 
  httplastrede bigint, 
  acklastconf bigint, 
  host string, 
  uri string, 
  xonlinehost string, 
  useragent string, 
  httpcontenttype string, 
  referuri string, 
  cookie string, 
  contentlength bigint, 
  targetbehavior bigint, 
  wtpinterrupttype bigint, 
  wtpinterruptcause bigint, 
  title string, 
  keyword string, 
  busconductlogo bigint, 
  buscompletionflag bigint, 
  busrede bigint, 
  browsingtool bigint, 
  portalapp bigint, 
  rangetime string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/s1u_http_orgn';

drop table tb_xdr_ifc_dns;
CREATE EXTERNAL TABLE `tb_xdr_ifc_dns`(
  `length` int,
  `city` int,
  `interface` int,
  `xdrid` string,
  `rat` int,
  `imsi` bigint,
  `imei` bigint,
  `msisdn` bigint,
  `machineipaddtype` int,
  `sgwipaddr` string,
  `enbipaddr` string,
  `sgwport` int,
  `enbport` int,
  `enbgtpteid` string,
  `sgwgtpteid` bigint,
  `tac` bigint,
  `ecgi` bigint,
  `apn` string,
  `apptypecode` bigint,
  `procedurestarttime` bigint,
  `procedureendtime` bigint,
  `protocoltype` bigint,
  `apptype` bigint,
  `appsubtype` bigint,
  `appcontent` int,
  `appstatus` int,
  `useripv4` string,
  `useripv6` string,
  `userport` string,
  `l4protocal` int,
  `appserveripipv4` string,
  `appserveripipv6` string,
  `appserverport` string,
  `uldata` bigint,
  `dldata` bigint,
  `ulippacket` bigint,
  `dlippacket` bigint,
  `ultcppacketor` bigint,
  `dltcppacketor` bigint,
  `ultcppacketre` bigint,
  `dltcppacketre` bigint,
  `tcpestabrede` bigint,
  `tcpestabdeconf` bigint,
  `ulipfragpackets` bigint,
  `dlipfragpackets` bigint,
  `tcpfirstrede` bigint,
  `tcpfirstconf` bigint,
  `winsize` bigint,
  `msssize` bigint,
  `tcpattnum` int,
  `tcplinkstatus` int,
  `sessionflag` int,
  `reqdns` string,
  `resultip` string,
  `dnsrecode` bigint,
  `dnsattnum` bigint,
  `dnsrenum` bigint,
  `licensedconnum` bigint,
  `additionalconnum` bigint,
  `parentxdrid` string)
PARTITIONED BY (
  `dt` string,
  `h` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  LOCATION
  '/${DB_PATH}/s1u_dns_orgn';

DROP TABLE IF EXISTS tb_xdr_ifc_mw; 
CREATE EXTERNAL TABLE tb_xdr_ifc_mw(
  length bigint, 
  city string, 
  interface bigint, 
  xdrid string, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype bigint, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  servicetype bigint, 
  procedurestatus bigint, 
  callingnumber string, 
  callednumber string, 
  callingpartyuri string, 
  requesturi string, 
  userip string, 
  callid string, 
  icid string, 
  sourceneip string, 
  sourceneport bigint, 
  destneip string, 
  destneport bigint, 
  callside bigint, 
  sourceaccesstype bigint, 
  sourceeci bigint, 
  sourcetac bigint, 
  sourceimsi bigint, 
  sourceimei bigint, 
  destaccesstype bigint, 
  desteci bigint, 
  desttac bigint, 
  destimsi bigint, 
  destimei bigint, 
  authtype bigint, 
  expirestimereq bigint, 
  expirestimersp bigint, 
  callingsdpipaddr string, 
  callingaudiosdpport bigint, 
  callingvideosdpport bigint, 
  calledsdpipaddr bigint, 
  calledaudiosdpport bigint, 
  calledvideoport bigint, 
  audiocodec bigint, 
  videocodec bigint, 
  redirectingpartyaddress string, 
  originalpartyaddress string, 
  redirectreason bigint, 
  responsecode bigint, 
  finishwarningcode bigint, 
  finishreasonprotocol bigint, 
  finishreasoncause bigint, 
  firfailtime bigint, 
  firstfailneip string, 
  first_fail_transaction string, 
  alertingtime bigint, 
  answertime bigint, 
  releasetime bigint, 
  callduration bigint, 
  authreqtime bigint, 
  authrsptime bigint, 
  stnsr string, 
  atcfmgmt string, 
  atusti string, 
  cmsisdn string, 
  ssi string, 
  rangetime string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/volte_orgn';

DROP TABLE IF EXISTS tb_xdr_ifc_s1mme; 
CREATE EXTERNAL TABLE tb_xdr_ifc_s1mme(
  length bigint, 
  city string, 
  interface int, 
  xdrid string, 
  rat int, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype int, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  procedurestatus bigint, 
  requestcause bigint, 
  failurecause bigint, 
  keyword1 bigint, 
  keyword2 bigint, 
  keyword3 bigint, 
  keyword4 bigint, 
  mmeues1apid bigint, 
  oldmmegroupid bigint, 
  oldmmecode bigint, 
  oldmtmsi bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  mtmsi bigint, 
  tmsi bigint, 
  useripv4 string, 
  useripv6 string, 
  mmeipadd string, 
  enbipadd string, 
  mmeport bigint, 
  enbport bigint, 
  tac bigint, 
  cellid bigint, 
  othertac bigint, 
  othereci bigint, 
  apn string, 
  epsbearernumber bigint, 
  bearer0id bigint, 
  bearer0type bigint, 
  bearer0qci bigint, 
  bearer0status bigint, 
  bearer0requestcause bigint, 
  bearer0failurecause bigint, 
  bearer0enbgtpteid bigint, 
  bearer0sgwgtpteid bigint, 
  bearer1id bigint, 
  bearer1type bigint, 
  bearer1qci bigint, 
  bearer1status bigint, 
  bearer1requestcause bigint, 
  bearer1failurecause bigint, 
  bearer1enbgtpteid bigint, 
  bearer1sgwgtpteid bigint, 
  bearer2id bigint, 
  bearer2type bigint, 
  bearer2qci bigint, 
  bearer2status bigint, 
  bearer2requestcause bigint, 
  bearer2failurecause bigint, 
  bearer2enbgtpteid bigint, 
  bearer2sgwgtpteid bigint, 
  bearer3id bigint, 
  bearer3type bigint, 
  bearer3qci bigint, 
  bearer3status bigint, 
  bearer3requestcause bigint, 
  bearer3failurecause bigint, 
  bearer3enbgtpteid bigint, 
  bearer3sgwgtpteid bigint, 
  bearer4id bigint, 
  bearer4type bigint, 
  bearer4qci bigint, 
  bearer4status bigint, 
  bearer4requestcause bigint, 
  bearer4failurecause bigint, 
  bearer4enbgtpteid bigint, 
  bearer4sgwgtpteid bigint, 
  bearer5id bigint, 
  bearer5type bigint, 
  bearer5qci bigint, 
  bearer5status bigint, 
  bearer5requestcause bigint, 
  bearer5failurecause bigint, 
  bearer5enbgtpteid bigint, 
  bearer5sgwgtpteid bigint, 
  bearer6id bigint, 
  bearer6type bigint, 
  bearer6qci bigint, 
  bearer6status bigint, 
  bearer6requestcause bigint, 
  bearer6failurecause bigint, 
  bearer6enbgtpteid bigint, 
  bearer6sgwgtpteid bigint, 
  bearer7id bigint, 
  bearer7type bigint, 
  bearer7qci bigint, 
  bearer7status bigint, 
  bearer7requestcause bigint, 
  bearer7failurecause bigint, 
  bearer7enbgtpteid bigint, 
  bearer7sgwgtpteid bigint, 
  bearer8id bigint, 
  bearer8type bigint, 
  bearer8qci bigint, 
  bearer8status bigint, 
  bearer8requestcause bigint, 
  bearer8failurecause bigint, 
  bearer8enbgtpteid bigint, 
  bearer8sgwgtpteid bigint, 
  bearer9id bigint, 
  bearer9type bigint, 
  bearer9qci bigint, 
  bearer9status bigint, 
  bearer9requestcause bigint, 
  bearer9failurecause bigint, 
  bearer9enbgtpteid bigint, 
  bearer9sgwgtpteid bigint, 
  bearer10id bigint, 
  bearer10type bigint, 
  bearer10qci bigint, 
  bearer10status bigint, 
  bearer10requestcause bigint, 
  bearer10failurecause bigint, 
  bearer10enbgtpteid bigint, 
  bearer10sgwgtpteid bigint, 
  bearer11id bigint, 
  bearer11type bigint, 
  bearer11qci bigint, 
  bearer11status bigint, 
  bearer11requestcause bigint, 
  bearer11failurecause bigint, 
  bearer11enbgtpteid bigint, 
  bearer11sgwgtpteid bigint, 
  bearer12id bigint, 
  bearer12type bigint, 
  bearer12qci bigint, 
  bearer12status bigint, 
  bearer12requestcause bigint, 
  bearer12failurecause bigint, 
  bearer12enbgtpteid bigint, 
  bearer12sgwgtpteid bigint, 
  bearer13id bigint, 
  bearer13type bigint, 
  bearer13qci bigint, 
  bearer13status bigint, 
  bearer13requestcause bigint, 
  bearer13failurecause bigint, 
  bearer13enbgtpteid bigint, 
  bearer13sgwgtpteid bigint, 
  bearer14id bigint, 
  bearer14type bigint, 
  bearer14qci bigint, 
  bearer14status bigint, 
  bearer14requestcause bigint, 
  bearer14failurecause bigint, 
  bearer14enbgtpteid bigint, 
  bearer14sgwgtpteid bigint, 
  bearer15id bigint, 
  bearer15type bigint, 
  bearer15qci bigint, 
  bearer15status bigint, 
  bearer15requestcause bigint, 
  bearer15failurecause bigint, 
  bearer15enbgtpteid bigint, 
  bearer15sgwgtpteid bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/s1mme_orgn';

DROP TABLE IF EXISTS tb_xdr_ifc_sv;  
CREATE EXTERNAL TABLE tb_xdr_ifc_sv(
  length bigint, 
  city string, 
  interface bigint, 
  xdrid string, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype bigint, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  sourceneip string, 
  sourceneport bigint, 
  destneip string, 
  destneport bigint, 
  roamdirection bigint, 
  homemcc bigint, 
  homemnc bigint, 
  mcc bigint, 
  mnc bigint, 
  targetlac bigint, 
  sourcetac bigint, 
  sourceeci bigint, 
  svflags bigint, 
  ulcmscip string, 
  dlcmmeip string, 
  ulcmscteid bigint, 
  dlcmmeteid bigint, 
  stnsr string, 
  targetrncid bigint, 
  targetcellid bigint, 
  arp bigint, 
  requestresult bigint, 
  result bigint, 
  svcause bigint, 
  postfailurecause bigint, 
  respdelay bigint, 
  svdelay bigint, 
  rangetime string)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/volte_sv';

DROP TABLE IF EXISTS tb_xdr_ifc_uu;   
CREATE EXTERNAL TABLE tb_xdr_ifc_uu(
  parentid string, 
  length bigint, 
  city string, 
  interface int, 
  xdrid string, 
  rat int, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype int, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  keyword1 int, 
  keyword2 int, 
  procedurestatus int, 
  plmnid string, 
  enbid bigint, 
  cellid bigint, 
  crnti bigint, 
  targetenbid bigint, 
  targetcellid bigint, 
  targetcrnti bigint, 
  mmeues1apid bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  mtmsi bigint, 
  csfbindication bigint, 
  redirectednetwork bigint, 
  epsbearernumber int, 
  bearer0id bigint, 
  bearer0status bigint, 
  bearer1id bigint, 
  bearer1status bigint, 
  bearer2id bigint, 
  bearer2status bigint, 
  bearer3id bigint, 
  bearer3status bigint, 
  bearer4id bigint, 
  bearer4status bigint, 
  bearer5id bigint, 
  bearer5status bigint, 
  bearer6id bigint, 
  bearer6status bigint, 
  bearer7id bigint, 
  bearer7status bigint, 
  bearer8id bigint, 
  bearer8status bigint, 
  bearer9id bigint, 
  bearer9status bigint, 
  bearer10id bigint, 
  bearer10status bigint, 
  bearer11id bigint, 
  bearer11status bigint, 
  bearer12id bigint, 
  bearer12status bigint, 
  bearer13id bigint, 
  bearer13status bigint, 
  bearer14id bigint, 
  bearer14status bigint, 
  bearer15id bigint, 
  bearer15status bigint, 
  rangetime string, 
  etype int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/TB_XDR_IFC_UU';
  
DROP TABLE IF EXISTS tb_xdr_ifc_x2;   
CREATE EXTERNAL TABLE tb_xdr_ifc_x2(
  parentid string, 
  length bigint, 
  city string, 
  interface int, 
  xdrid string, 
  rat int, 
  imsi string, 
  imei string, 
  msisdn string, 
  proceduretype int, 
  procedurestarttime bigint, 
  procedureendtime bigint, 
  procedurestatus int, 
  cellid bigint, 
  targetcellid bigint, 
  enbid bigint, 
  targetenbid bigint, 
  mmeues1apid bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  requestcause bigint, 
  failurecause bigint, 
  epsbearernumber bigint, 
  bearer0id bigint, 
  bearer0status bigint, 
  bearer1id bigint, 
  bearer1status bigint, 
  bearer2id bigint, 
  bearer2status bigint, 
  bearer3id bigint, 
  bearer3status bigint, 
  bearer4id bigint, 
  bearer4status bigint, 
  bearer5id bigint, 
  bearer5status bigint, 
  bearer6id bigint, 
  bearer6status bigint, 
  bearer7id bigint, 
  bearer7status bigint, 
  bearer8id bigint, 
  bearer8status bigint, 
  bearer9id bigint, 
  bearer9status bigint, 
  bearer10id bigint, 
  bearer10status bigint, 
  bearer11id bigint, 
  bearer11status bigint, 
  bearer12id bigint, 
  bearer12status bigint, 
  bearer13id bigint, 
  bearer13status bigint, 
  bearer14id bigint, 
  bearer14status bigint, 
  bearer15id bigint, 
  bearer15status bigint, 
  rangetime string, 
  etype int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/TB_XDR_IFC_X2';

DROP TABLE IF EXISTS lte_mro_source;  
CREATE EXTERNAL TABLE lte_mro_source(
  objectid string, 
  vid bigint, 
  fileformatversion string, 
  starttime string, 
  endtime string, 
  period bigint, 
  enbid bigint, 
  userlabel string, 
  mrname string, 
  cellid bigint, 
  earfcn bigint, 
  subframenbr bigint, 
  prbnbr bigint, 
  mmeues1apid bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  meatime string, 
  eventtype string, 
  gridcenterlongitude string, 
  gridcenterlatitude string, 
  kpi1 bigint, 
  kpi2 bigint, 
  kpi3 bigint, 
  kpi4 bigint, 
  kpi5 bigint, 
  kpi6 bigint, 
  kpi7 bigint, 
  kpi8 bigint, 
  kpi9 bigint, 
  kpi10 bigint, 
  kpi11 bigint, 
  kpi12 bigint, 
  kpi13 bigint, 
  kpi14 bigint, 
  kpi15 bigint, 
  kpi16 bigint, 
  kpi17 bigint, 
  kpi18 bigint, 
  kpi19 bigint, 
  kpi20 bigint, 
  kpi21 bigint, 
  kpi22 bigint, 
  kpi23 bigint, 
  kpi24 bigint, 
  kpi25 bigint, 
  kpi26 bigint, 
  kpi27 bigint, 
  kpi28 bigint, 
  kpi29 bigint, 
  kpi30 bigint, 
  kpi31 bigint, 
  kpi32 bigint, 
  kpi33 bigint, 
  kpi34 bigint, 
  kpi35 bigint, 
  kpi36 bigint, 
  kpi37 bigint, 
  kpi38 bigint, 
  kpi39 bigint, 
  kpi40 bigint, 
  kpi41 bigint, 
  kpi42 bigint, 
  kpi43 bigint, 
  kpi44 bigint, 
  kpi45 bigint, 
  kpi46 bigint, 
  kpi47 bigint, 
  kpi48 bigint, 
  kpi49 bigint, 
  kpi50 bigint, 
  kpi51 bigint, 
  kpi52 bigint, 
  kpi53 bigint, 
  kpi54 bigint, 
  kpi55 bigint, 
  kpi56 bigint, 
  kpi57 bigint, 
  kpi58 bigint, 
  kpi59 bigint, 
  kpi60 bigint, 
  kpi61 bigint, 
  kpi62 bigint, 
  kpi63 bigint, 
  kpi64 bigint, 
  kpi65 bigint, 
  kpi66 bigint, 
  kpi67 bigint, 
  kpi68 bigint, 
  kpi69 bigint, 
  kpi70 bigint, 
  kpi71 bigint, 
  length bigint, 
  city string, 
  xdrtype bigint, 
  interface bigint, 
  xdrid string, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  mrtype bigint, 
  neighborcellnumber bigint, 
  gsmneighborcellnumber bigint, 
  tdsneighborcellnumber bigint, 
  v_enb bigint, 
  mrtime bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/${DB_PATH}/LTE_MRO_SOURCE';

CREATE DATABASE IF NOT EXISTS ${DCLDB};
USE ${DCLDB};

DROP TABLE IF EXISTS cell_mr;  
CREATE  TABLE cell_mr(
  objectid string, 
  vid bigint, 
  fileformatversion string, 
  starttime string, 
  endtime string, 
  period bigint, 
  enbid bigint, 
  userlabel string, 
  mrname string, 
  cellid bigint, 
  earfcn bigint, 
  subframenbr bigint, 
  prbnbr bigint, 
  mmeues1apid bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  meatime string, 
  eventtype string, 
  gridcenterlongitude string, 
  gridcenterlatitude string, 
  kpi1 bigint, 
  kpi2 bigint, 
  kpi3 bigint, 
  kpi4 bigint, 
  kpi5 bigint, 
  kpi6 bigint, 
  kpi7 bigint, 
  kpi8 bigint, 
  kpi9 bigint, 
  kpi10 bigint, 
  kpi11 bigint, 
  kpi12 bigint, 
  kpi13 bigint, 
  kpi14 bigint, 
  kpi15 bigint, 
  kpi16 bigint, 
  kpi17 bigint, 
  kpi18 bigint, 
  kpi19 bigint, 
  kpi20 bigint, 
  kpi21 bigint, 
  kpi22 bigint, 
  kpi23 bigint, 
  kpi24 bigint, 
  kpi25 bigint, 
  kpi26 bigint, 
  kpi27 bigint, 
  kpi28 bigint, 
  kpi29 bigint, 
  kpi30 bigint, 
  kpi31 bigint, 
  kpi32 bigint, 
  kpi33 bigint, 
  kpi34 bigint, 
  kpi35 bigint, 
  kpi36 bigint, 
  kpi37 bigint, 
  kpi38 bigint, 
  kpi39 bigint, 
  kpi40 bigint, 
  kpi41 bigint, 
  kpi42 bigint, 
  kpi43 bigint, 
  kpi44 bigint, 
  kpi45 bigint, 
  kpi46 bigint, 
  kpi47 bigint, 
  kpi48 bigint, 
  kpi49 bigint, 
  kpi50 bigint, 
  kpi51 bigint, 
  kpi52 bigint, 
  kpi53 bigint, 
  kpi54 bigint, 
  kpi55 bigint, 
  kpi56 bigint, 
  kpi57 bigint, 
  kpi58 bigint, 
  kpi59 bigint, 
  kpi60 bigint, 
  kpi61 bigint, 
  kpi62 bigint, 
  kpi63 bigint, 
  kpi64 bigint, 
  kpi65 bigint, 
  kpi66 bigint, 
  kpi67 bigint, 
  kpi68 bigint, 
  kpi69 bigint, 
  kpi70 bigint, 
  kpi71 bigint, 
  length bigint, 
  city string, 
  xdrtype bigint, 
  interface bigint, 
  xdrid string, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  mrtype bigint, 
  neighborcellnumber bigint, 
  gsmneighborcellnumber bigint, 
  tdsneighborcellnumber bigint, 
  v_enb bigint, 
  mrtime bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

DROP TABLE IF EXISTS exception_analysis;    
CREATE  TABLE exception_analysis(
  event_name string, 
  procedurestarttime bigint, 
  imsi string, 
  proceduretype string, 
  etype int, 
  cellid string, 
  falurecause string, 
  celltype string, 
  cellregion string, 
  cellkey string, 
  interface string, 
  prointerface string, 
  rangetime string, 
  elong double, 
  elat double, 
  eupordown int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',';

DROP TABLE IF EXISTS lte_mro_source;   
CREATE  TABLE lte_mro_source(
  objectid string, 
  vid bigint, 
  fileformatversion string, 
  starttime string, 
  endtime string, 
  period bigint, 
  enbid bigint, 
  userlabel string, 
  mrname string, 
  cellid bigint, 
  earfcn bigint, 
  subframenbr bigint, 
  prbnbr bigint, 
  mmeues1apid bigint, 
  mmegroupid bigint, 
  mmecode bigint, 
  meatime string, 
  eventtype string, 
  gridcenterlongitude string, 
  gridcenterlatitude string, 
  kpi1 bigint, 
  kpi2 bigint, 
  kpi3 bigint, 
  kpi4 bigint, 
  kpi5 bigint, 
  kpi6 bigint, 
  kpi7 bigint, 
  kpi8 bigint, 
  kpi9 bigint, 
  kpi10 bigint, 
  kpi11 bigint, 
  kpi12 bigint, 
  kpi13 bigint, 
  kpi14 bigint, 
  kpi15 bigint, 
  kpi16 bigint, 
  kpi17 bigint, 
  kpi18 bigint, 
  kpi19 bigint, 
  kpi20 bigint, 
  kpi21 bigint, 
  kpi22 bigint, 
  kpi23 bigint, 
  kpi24 bigint, 
  kpi25 bigint, 
  kpi26 bigint, 
  kpi27 bigint, 
  kpi28 bigint, 
  kpi29 bigint, 
  kpi30 bigint, 
  kpi31 bigint, 
  kpi32 bigint, 
  kpi33 bigint, 
  kpi34 bigint, 
  kpi35 bigint, 
  kpi36 bigint, 
  kpi37 bigint, 
  kpi38 bigint, 
  kpi39 bigint, 
  kpi40 bigint, 
  kpi41 bigint, 
  kpi42 bigint, 
  kpi43 bigint, 
  kpi44 bigint, 
  kpi45 bigint, 
  kpi46 bigint, 
  kpi47 bigint, 
  kpi48 bigint, 
  kpi49 bigint, 
  kpi50 bigint, 
  kpi51 bigint, 
  kpi52 bigint, 
  kpi53 bigint, 
  kpi54 bigint, 
  kpi55 bigint, 
  kpi56 bigint, 
  kpi57 bigint, 
  kpi58 bigint, 
  kpi59 bigint, 
  kpi60 bigint, 
  kpi61 bigint, 
  kpi62 bigint, 
  kpi63 bigint, 
  kpi64 bigint, 
  kpi65 bigint, 
  kpi66 bigint, 
  kpi67 bigint, 
  kpi68 bigint, 
  kpi69 bigint, 
  kpi70 bigint, 
  kpi71 bigint, 
  length bigint, 
  city string, 
  xdrtype bigint, 
  interface bigint, 
  xdrid string, 
  rat bigint, 
  imsi string, 
  imei string, 
  msisdn string, 
  mrtype bigint, 
  neighborcellnumber bigint, 
  gsmneighborcellnumber bigint, 
  tdsneighborcellnumber bigint, 
  v_enb bigint, 
  mrtime bigint)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

DROP TABLE IF EXISTS mr_gt_cell_ana_base60;    
CREATE  TABLE mr_gt_cell_ana_base60(
  cellid int, 
  ttime string, 
  dir_state int, 
  avgrsrpx int, 
  commy int, 
  avgrsrqx int, 
  ltecoverratex int, 
  weakcoverratex int, 
  overlapcoverratex int, 
  overlapcoverratey int, 
  upsigrateavgx int, 
  upsigrateavgy int, 
  updiststrox int, 
  updiststroy int, 
  model3diststrox int, 
  model3diststroy int, 
  uebootx int, 
  uebooty int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

DROP TABLE IF EXISTS mr_gt_cell_ana_baseday;  
CREATE  TABLE mr_gt_cell_ana_baseday(
  cellid int, 
  ttime string, 
  dir_state int, 
  avgrsrpx int, 
  commy int, 
  avgrsrqx int, 
  ltecoverratex int, 
  weakcoverratex int, 
  overlapcoverratex int, 
  overlapcoverratey int, 
  upsigrateavgx int, 
  upsigrateavgy int, 
  updiststrox int, 
  updiststroy int, 
  model3diststrox int, 
  model3diststroy int, 
  uebootx int, 
  uebooty int)
PARTITIONED BY ( 
  dt string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',';

DROP TABLE IF EXISTS mr_gt_user_ana_base60;    
CREATE  TABLE mr_gt_user_ana_base60(
  imsi string, 
  imei string, 
  msisdn string, 
  cellid int, 
  rruid string, 
  gridid int, 
  ttime string, 
  dir_state int, 
  elong int, 
  elat int, 
  avgrsrpx int, 
  commy int, 
  avgrsrqx int, 
  ltecoverratex int, 
  weakcoverratex int, 
  overlapcoverratex int, 
  overlapcoverratey int, 
  upsigrateavgx int, 
  upsigrateavgy int, 
  updiststrox int, 
  updiststroy int, 
  model3diststrox int, 
  model3diststroy int, 
  uebootx int, 
  uebooty int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;
 
DROP TABLE IF EXISTS mr_gt_user_ana_baseday;     
CREATE  TABLE mr_gt_user_ana_baseday(
  imsi string, 
  imei string, 
  msisdn string, 
  cellid int, 
  rruid string, 
  gridid int, 
  ttime string, 
  dir_state int, 
  elong int, 
  elat int, 
  avgrsrpx int, 
  commy int, 
  avgrsrqx int, 
  ltecoverratex int, 
  weakcoverratex int, 
  overlapcoverratex int, 
  overlapcoverratey int, 
  upsigrateavgx int, 
  upsigrateavgy int, 
  updiststrox int, 
  updiststroy int, 
  model3diststrox int, 
  model3diststroy int, 
  uebootx int, 
  uebooty int)
PARTITIONED BY ( 
  dt string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

DROP TABLE IF EXISTS volte_gt_cell_ana_base60; 
CREATE  TABLE volte_gt_cell_ana_base60(
  ttime string, 
  cellid int, 
  voltemcsucc int,
voltemcatt int,
voltevdsucc int,
voltevdatt int,
voltetime double,
voltemctime double,
voltemctimey int,
voltevdtime double,
voltevdtimey int,
voltemchandover int,
volteanswer int,
voltevdhandover int,
voltevdanswer int,
srvccsucc int,
srvccatt int,
srvcctime int,
lteswsucc int,
lteswatt int,
srqatt int,
srqsucc int,
tauatt int,
tausucc int,
rrcrebuild int,
rrcsucc int,
rrcreq int,
imsiregatt int,
imsiregsucc int,
wirelessdrop int,
wireless int,
eabdrop int,
eab int,
eabs1swx int,
eabs1swy int,
s1tox2swx int,
s1tox2swy int,
enbx2swx int,
enbx2swy int,
uuenbswx int,
uuenbswy int,
uuenbinx int,
uuenbiny int,
swx int,
swy int,
attachx int,
attachy int,
voltesucc int,
srvccsuccS1 int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;

DROP TABLE IF EXISTS volte_gt_cell_ana_baseday;   
CREATE  TABLE volte_gt_cell_ana_baseday(
  ttime string, 
  cellid int, 
  voltemcsucc int,
voltemcatt int,
voltevdsucc int,
voltevdatt int,
voltetime double,
voltemctime double,
voltemctimey int,
voltevdtime double,
voltevdtimey int,
voltemchandover int,
volteanswer int,
voltevdhandover int,
voltevdanswer int,
srvccsucc int,
srvccatt int,
srvcctime int,
lteswsucc int,
lteswatt int,
srqatt int,
srqsucc int,
tauatt int,
tausucc int,
rrcrebuild int,
rrcsucc int,
rrcreq int,
imsiregatt int,
imsiregsucc int,
wirelessdrop int,
wireless int,
eabdrop int,
eab int,
eabs1swx int,
eabs1swy int,
s1tox2swx int,
s1tox2swy int,
enbx2swx int,
enbx2swy int,
uuenbswx int,
uuenbswy int,
uuenbinx int,
uuenbiny int,
swx int,
swy int,
attachx int,
attachy int,
voltesucc int,
srvccsuccS1 int)
PARTITIONED BY ( 
  dt string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',';

DROP TABLE IF EXISTS volte_gt_user_ana_base60;   
CREATE  TABLE volte_gt_user_ana_base60(
  imsi string, 
  imei string, 
  msisdn string, 
  cellid int, 
  ttime string, 
  voltemcsucc int,
voltemcatt int,
voltevdsucc int,
voltevdatt int,
voltetime double,
voltemctime double,
voltemctimey int,
voltevdtime double,
voltevdtimey int,
voltemchandover int,
volteanswer int,
voltevdhandover int,
voltevdanswer int,
srvccsucc int,
srvccatt int,
srvcctime int,
lteswsucc int,
lteswatt int,
srqatt int,
srqsucc int,
tauatt int,
tausucc int,
rrcrebuild int,
rrcsucc int,
rrcreq int,
imsiregatt int,
imsiregsucc int,
wirelessdrop int,
wireless int,
eabdrop int,
eab int,
eabs1swx int,
eabs1swy int,
s1tox2swx int,
s1tox2swy int,
enbx2swx int,
enbx2swy int,
uuenbswx int,
uuenbswy int,
uuenbinx int,
uuenbiny int,
swx int,
swy int,
attachx int,
attachy int,
voltesucc int,
srvccsuccS1 int)
PARTITIONED BY ( 
  dt string, 
  h string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' ;
  
DROP TABLE IF EXISTS volte_gt_user_ana_baseday;    
CREATE  TABLE volte_gt_user_ana_baseday(
  imsi string, 
  imei string, 
  msisdn string, 
  cellid int, 
  ttime string, 
  voltemcsucc int,
voltemcatt int,
voltevdsucc int,
voltevdatt int,
voltetime double,
voltemctime double,
voltemctimey int,
voltevdtime double,
voltevdtimey int,
voltemchandover int,
volteanswer int,
voltevdhandover int,
voltevdanswer int,
srvccsucc int,
srvccatt int,
srvcctime int,
lteswsucc int,
lteswatt int,
srqatt int,
srqsucc int,
tauatt int,
tausucc int,
rrcrebuild int,
rrcsucc int,
rrcreq int,
imsiregatt int,
imsiregsucc int,
wirelessdrop int,
wireless int,
eabdrop int,
eab int,
eabs1swx int,
eabs1swy int,
s1tox2swx int,
s1tox2swy int,
enbx2swx int,
enbx2swy int,
uuenbswx int,
uuenbswy int,
uuenbinx int,
uuenbiny int,
swx int,
swy int,
attachx int,
attachy int,
voltesucc int,
srvccsuccS1 int)
PARTITIONED BY ( 
  dt string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',';
EOF
