#!/bin/bash
ANALY_DATE=$1
ANALY_HOUR=$2
DB=$3


hive<<EOF
USE ${DB};

alter table lte_mro_source drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table lte_mro_source add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_gxrx drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_gxrx add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_GXRX/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_uu drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_uu add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_x2 drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_x2 add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}";


alter table tb_xdr_ifc_sv drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_sv add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_SV/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_s1mme drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_s1mme add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_S1MME/${ANALY_DATE}/${ANALY_HOUR}";

alter table tb_xdr_ifc_mw drop partition(dt="$ANALY_DATE",h="$ANALY_HOUR");
alter table tb_xdr_ifc_mw add partition(dt="$ANALY_DATE",h="$ANALY_HOUR") 
location "/datang/TB_XDR_IFC_MW/${ANALY_DATE}/${ANALY_HOUR}";
EOF

