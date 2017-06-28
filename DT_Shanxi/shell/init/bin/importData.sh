#!/bin/bash
sh hdfs2db.sh /datang/output/xdrnew/gx/ tb_xdr_ifc_gxrx_new 23 4
sh hdfs2db.sh /datang/output/xdrnew/ltemrosource/lte_mro_source_new 26 4
sh hdfs2db.sh /datang/output/xdrnew/mw/tb_xdr_ifc_mw_new* tb_xdr_ifc_gmmwmgmimjisc_new 19 4
sh hdfs2db.sh /datang/output/xdrnew/uu tb_xdr_ifc_uu_new 21 4
sh hdfs2db.sh /datang/output/xdrnew/x2 tb_xdr_ifc_x2_new 22 4
sh hdfs2db.sh /datang/output/xdrnew/s1mme TB_XDR_IFC_S1MME_NEW 20 1
sh hdfs2db.sh /datang/output/xdrnew/sgs TB_XDR_IFC_SGS_NEW 24 1
sh hdfs2db.sh /datang/output/xdrnew/sv TB_XDR_IFC_SV_NEW 25 1
#sh hdfs2db.sh /datang/output/dcl.db/mr_gt_cell_ana_baseday mr_gt_cell_ana_baseday 16 4
#sh hdfs2db.sh /datang/output/dcl.db/mr_gt_user_ana_base60 mr_gt_user_ana_base60 17 4
#sh hdfs2db.sh /datang/output/dcl.db/mr_gt_user_ana_baseday mr_gt_user_ana_baseday 18 4
#sh hdfs2db.sh /user/hive/warehouse/dcl.db/volte_gt_cell_ana_base60 volte_gt_cell_ana_base60 11 4
#sh hdfs2db.sh /user/hive/warehouse/dcl.db/volte_gt_cell_ana_baseday volte_gt_cell_ana_baseday 12 4
#sh hdfs2db.sh /user/hive/warehouse/dcl.db/volte_gt_user_ana_base60 volte_gt_user_ana_base60 13 4
sh hdfs2db.sh /datang/output/business/loc_guser_mark* loc_guser_mark 8 4
sh hdfs2db.sh /datang/output/business/VOLTE_GT_BUSI_USER* VOLTE_GT_BUSI_USER_data 9 4
sh hdfs2db.sh /datang/output/business/COMM_USER_DATA* COMM_USER_DATA 10 1
sh hdfs2db.sh /datang/output/free/VOLTE_GT_FREE_USER* VOLTE_GT_FREE_USER_data 30 1
sh hdfs2db.sh /datang/output/xdrnew/mw/volte_gtuser_data* volte_gtuser_data 28 1

