#!/bin/bash

ANALY_DATE=20170105

for (( ANALY_HOUR=15;ANALY_HOUR<=17;ANALY_HOUR++ ))
do


./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/tb* tb_xdr_ifc_gmmwmgmimjisc_new 19 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/s1mme/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_s1mme_new 20 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/uu/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_uu_new 21 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/x2/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_x2_new 22 2   
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sv_xdr/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sv_new 25 4   
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/gxrx/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_gxrx_new 23 2  
#./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/sgs/${ANALY_DATE}/${ANALY_HOUR} tb_xdr_ifc_sgs_new 24 4   
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/ltemrosource/${ANALY_DATE}/${ANALY_HOUR} lte_mro_source_new 26 2  
#./hdfs2db.sh hdfs://dtcluster/datang2/output/userHome/${ANALY_DATE}/${ANALY_HOUR} user_home 31 4   
#./hdfs2db.sh hdfs://dtcluster/datang2/output/trainiden/${ANALY_DATE}/${ANALY_HOUR} numberiden 35 4   
./hdfs2db.sh hdfs://dtcluster/datang2/output/xdrnew/mw/${ANALY_DATE}/${ANALY_HOUR}/volte* volte_gtuser_data 28 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/u1/${ANALY_DATE}/${ANALY_HOUR} u1 27 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/u2/${ANALY_DATE}/${ANALY_HOUR} u2 27 2   
./hdfs2db.sh hdfs://dtcluster/datang2/output/u3/${ANALY_DATE}/${ANALY_HOUR} u3 27 2    
#./hdfs2db.sh hdfs://dtcluster/datang2/output/u4/${ANALY_DATE}/u4* u4 27 2   
#./hdfs2db.sh hdfs://dtcluster/datang2/output/updowntrain/${ANALY_DATE} upordown 29 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/loc_guser_mark* loc_guser_mark 8 2
./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_FREE_USER* volte_gt_free_user_data 30 2 
./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_GT_BUSI_USER* volte_gt_busi_user_data 9 2   
./hdfs2db.sh hdfs://dtcluster/datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2
#./hdfs2db.sh hdfs://dtcluster/datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/COMM_USER_DATA* comm_user_data 10 2  
./hdfs2db.sh hdfs://dtcluster/datang2/cellMR/${ANALY_DATE}/${ANALY_HOUR} VOLTE_CELLMR_DATA 32 2
done 
