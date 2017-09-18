#!/bin/bash

DDB=$1

hive<<EOF
create database if not exists ${DDB};
use ${DDB};
drop table if exists tb_xdr_ifc_s1mme;
CREATE TABLE if not exists tb_xdr_ifc_s1mme(
  length_s int,
  local_province_s string,
  local_city_s string,
  owner_province_s string,
  owner_city_s string,
  roaming_type_s smallint,
  interface_s smallint,
  xdrid_s string,
  rat_s smallint,
  imsi_s string,
  imei_s string,
  msisdn_s string,
  procedure_type_s smallint,
  start_time_s bigint,
  end_time_s bigint,
  start_lon_s string,
  start_lat_s string,
  location_source_s smallint,
  msgflag_s smallint,
  procedure_status_s smallint,
  req_cause_g_s smallint,
  request_cause_s smallint,
  fail_cause_g_s smallint,
  failure_cause_s smallint,
  keyword1_s smallint,
  keyword2_s smallint,
  keyword3_s smallint,
  keyword4_s smallint,
  mme_ue_s1apid_s bigint,
  old_mme_groupid_s int,
  old_mmecode_s smallint,
  old_mtmsi_s bigint,
  old_guti_type_s smallint,
  mme_groupid_s int,
  mmecode_s smallint,
  mtmsi_s bigint,
  tmsi_s bigint,
  useripv4_s string,
  useripv6_s string,
  mme_ip_s string,
  enb_ip_s string,
  mmeport_s int,
  enbport_s int,
  tac_s int,
  eci_s bigint,
  other_tac_s int,
  other_eci_s bigint,
  apn_s string,
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
  h string,
  m string)
STORED AS PARQUET;

drop table if exists tb_xdr_ifc_uu;
CREATE TABLE if not exists tb_xdr_ifc_uu(
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
  h string,
  m string)
STORED AS PARQUET;

drop table if exists tb_xdr_ifc_x2;
CREATE TABLE if not exists tb_xdr_ifc_x2(
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
  h string,
  m string)
  STORED AS PARQUET;

drop table if exists tb_xdr_ifc_mw;
CREATE TABLE if not exists tb_xdr_ifc_mw(
  length_s int,
  city_s string,
  interface_s smallint,
  xdr_id_s string,
  rat_s smallint,
  imsi_s string,
  imei_s string,
  msisdn_s string,
  procedure_type_s smallint,
  procedure_start_time_s bigint,
  procedure_end_time_s bigint,
  service_type_s smallint,
  procedure_status_s smallint,
  calling_number_s string,
  called_number_s string,
  calling_party_uri_s string,
  request_uri_s string,
  user_ip_s string,
  callid_s string,
  icid_s string,
  source_ne_ip_s string,
  source_ne_port_s string,
  dest_ne_ip_s string,
  dest_ne_port_s int,
  call_side_s smallint,
  source_access_type_s smallint,
  source_eci_s bigint,
  source_tac_s int,
  source_imsi_s string,
  source_imei_s string,
  dest_access_type_s smallint,
  dest_eci_s bigint,
  dest_tac_s int,
  dest_imsi_s string,
  dest_imei_s string,
  auth_type_s smallint,
  expires_time_req_s bigint,
  expires_time_rsp_s bigint,
  calling_sdp_ip_addr_s string,
  calling_audio_sdp_port_s int,
  calling_video_sdp_port_s int,
  called_sdp_ip_addr_s string,
  called_audio_sdp_port_s string,
  called_video_port_s string,
  audio_codec_s smallint,
  video_codec_s smallint,
  redirecting_party_address_s string,
  original_party_address_s string,
  redirect_reason_s smallint,
  response_code_s int,
  finish_warning_code_s int,
  finish_reason_protocol_s smallint,
  finish_reason_cause_s int,
  first_fail_time_s bigint,
  first_fail_ne_ip_s string,
  first_fail_transaction_s smallint,
  progress_time_s bigint,
  update_time_s bigint,
  alerting_time_s bigint,
  answer_time_s bigint,
  release_time_s bigint,
  call_duration_s bigint,
  auth_req_time_s bigint,
  auth_rsp_time_s bigint,
  stn_sr_s string,
  atcf_mgmt_s string,
  atu_sti_s string,
  c_msisdn_s string,
  ssi_s string,
  sbc_domain_s string,
  multiparty_call_status_s smallint,
  retryafter_s smallint,
  release_part_s smallint,
  finish_warning_s string,
  finish_reason_s string,
  nonce_value_s int,
  auth_response_s int,
  media_s string,
  user_agent_s string,
  executed_service_s smallint,
  enb_ip_s string,
  egw_ip_s string)
PARTITIONED BY (
  dt string,
  h string,
  m string)
   STORED AS PARQUET;

drop table if exists tb_xdr_ifc_gxrx;
CREATE TABLE if not exists tb_xdr_ifc_gxrx(
  length_s int,
  city_s string,
  interface_s smallint,
  xdr_id_s string,
  rat_s smallint,
  imsi_s string,
  imei_s string,
  msisdn_s string,
  procedure_type_s string,
  procedure_start_time_s bigint,
  procedure_end_time_s bigint,
  icid_s string,
  origin_realm_s string,
  destination_realm_s string,
  origin_host_s string,
  destination_host_s string,
  sgsn_sgw_sig_ip_s string,
  af_app_id_s string,
  cc_request_type_s smallint,
  rx_request_type_s smallint,
  media_type_s smallint,
  abort_cause_s smallint,
  result_code_s bigint,
  experimental_result_code_s bigint,
  session_release_cause_s bigint,
  rule_failure_code_s smallint,
  session_id_s string,
  called_station_id_s string,
  framed_ipv6_prefix_s string,
  framed_ip_address_s string,
  source_eci_s bigint,
  source_tac_s int,
  source_ne_ip_s string,
  source_ne_port_s int,
  destination_ne_ip_s string,
  destination_ne_port_s int,
  qci_s int)
PARTITIONED BY (
  dt string,
  h string,
  m string)
  STORED AS PARQUET;

drop table if exists tb_xdr_ifc_sv;
CREATE TABLE if not exists tb_xdr_ifc_sv(
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
  h string,
  m string)
  STORED AS PARQUET;

drop table if exists lte_mro_source;
CREATE TABLE if not exists lte_mro_source(
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
  h string,
  m string)
STORED AS PARQUET;
EOF