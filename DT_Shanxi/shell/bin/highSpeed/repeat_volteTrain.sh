#!/bin/bash
# Run it every hour.
my_path="$(cd "$(dirname "$0")"; pwd)"
cd $my_path
jar_file="/dt/lib/dt_mobile.jar"

ANALY_DATE=20170427
ANALY_HOUR=13

jar_file="/dt/dt_mobile.jar"
# free
user_distinguish_S1mmeXdr="cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.VolteTrainS1mmeJob"
# busi
user_distinguish_uuXdr="cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.VolteTrainUuJob"

# parameters
process="/datang2/parameter/profess.txt"
ht_switch="/datang2/parameter/ht_switch.txt"
public="/datang2/parameter/public.txt"
ht_sw_distance="/datang2/parameter/ht_sw_distance.csv"
grid="/datang2/parameter/grid.csv"
high_speed="/datang2/parameter/highspeedcell.csv"
trainstations="/datang2/parameter/stations.csv"
trainstationtimes="/datang2/parameter/stationTimes.txt"
#phone_number="/datang2/parameter/phone_number.csv"


# high-speed rail
confSpeed=130
distance_center=50
dt=10

volteTrainAlalyse_business_input="/datang2/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}/*"
volteTrainAlalyse_free_input="/datang2/TB_XDR_IFC_S1MME/${ANALY_DATE}/${ANALY_HOUR}/*"

volteTrainAlalyse_output="/datang2/output"
volteTrainAlalyse_business_output="${volteTrainAlalyse_output}/business/${ANALY_DATE}/${ANALY_HOUR}"
volteTrainAlalyse_free_output="${volteTrainAlalyse_output}/free/${ANALY_DATE}/${ANALY_HOUR}"
bus_volte_gt_busi_user="${volteTrainAlalyse_business_output}/VOLTE_GT_BUSI_USER-r-00000"
bus_volte_common_user="${volteTrainAlalyse_business_output}/COMM_USER_DATA*"
bus_loc_mark_output="${volteTrainAlalyse_business_output}/loc_guser_mark*"

free_volte_gt_busi_user="${volteTrainAlalyse_free}/VOLTE_GT_BUSI_USER*"
free_volte_common_user="${volteTrainAlalyse_free}/COMM_USER_DATA*"
free_loc_mark_output="${volteTrainAlalyse_free}/loc_guser_mark*"

hadoop fs -rm -r ${volteTrainAlalyse_business_output}

echo "business table"
hadoop jar ${jar_file} ${user_distinguish_uuXdr} ${volteTrainAlalyse_business_input} ${volteTrainAlalyse_free_input} ${volteTrainAlalyse_business_output} ${process} ${public} ${ht_switch} ${ht_sw_distance} ${high_speed} ${confSpeed} ${dt}

x2_table="/datang2/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}/"
s1mme_table="/datang2/TB_XDR_IFC_S1MME/${ANALY_DATE}/${ANALY_HOUR}"
gx_table="/datang2/TB_XDR_IFC_GXRX/${ANALY_DATE}/${ANALY_HOUR}"
sv_table="/datang2/TB_XDR_IFC_SV/${ANALY_DATE}/${ANALY_HOUR}"
mw_table="/datang2/TB_XDR_IFC_GMMWMGMIMJISC/${ANALY_DATE}/${ANALY_HOUR}"
lte_mro_source="/datang2/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
sgs_table="/datang2/TB_XDR_IFC_SGS/${ANALY_DATE}/${ANALY_HOUR}"
http_table="/datang2/s1u_http_orgn/dt=${ANALY_DATE}/h=${ANALY_HOUR}"
xdr_new_output="/datang2/output/xdrnew"

echo "free table"
hadoop fs -rm -r ${volteTrainAlalyse_free_output}
hadoop jar ${jar_file} ${user_distinguish_S1mmeXdr} ${volteTrainAlalyse_free_input} ${volteTrainAlalyse_free_output} ${process} ${public} ${bus_volte_gt_busi_user} ${high_speed} ${confSpeed} ${dt}

#拷贝高铁用户数据
echo "====copy high User========="
hdfs dfs -rm -R /datang2/BusinessGtUser/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -mkdir -p /datang2/BusinessGtUser/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -cp /datang2/output/business/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_*  /datang2/BusinessGtUser/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -rm -R /datang2/FreeGtUser/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -mkdir -p /datang2/FreeGtUser/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -cp /datang2/output/free/${ANALY_DATE}/${ANALY_HOUR}/VOLTE_*  /datang2/FreeGtUser/${ANALY_DATE}/${ANALY_HOUR}


echo "eight new tables"

x2_out=${xdr_new_output}/tb_xdr_ifc_x2/${ANALY_DATE}/${ANALY_HOUR}
gxrx_out=${xdr_new_output}/tb_xdr_ifc_gxrx/${ANALY_DATE}/${ANALY_HOUR}
sv_xdr_out=${xdr_new_output}/tb_xdr_ifc_sv/${ANALY_DATE}/${ANALY_HOUR}
ltemrosource_out=${xdr_new_output}/lte_mro_source/${ANALY_DATE}/${ANALY_HOUR}
sgs_out=${xdr_new_output}/tb_xdr_ifc_sgs/${ANALY_DATE}/${ANALY_HOUR}
uu_out=${xdr_new_output}/tb_xdr_ifc_uu/${ANALY_DATE}/${ANALY_HOUR}
s1mme_new_output=${xdr_new_output}/tb_xdr_ifc_s1mme/${ANALY_DATE}/${ANALY_HOUR}
mw_out=${xdr_new_output}/tb_xdr_ifc_mw/${ANALY_DATE}/${ANALY_HOUR}
gt_user=${xdr_new_output}/volte_gtuser_data/${ANALY_DATE}/${ANALY_HOUR}
http_out=${xdr_new_output}/tb_xdr_ifc_http/${ANALY_DATE}/${ANALY_HOUR}


hdfs dfs -rm -R ${x2_out}
hdfs dfs -rm -R ${gxrx_out}
hdfs dfs -rm -R ${sv_xdr_out}
hdfs dfs -rm -R ${ltemrosource_out}
hdfs dfs -rm -R ${sgs_out}
hdfs dfs -rm -R ${uu_out}
hdfs dfs -rm -R ${s1mme_new_output}
hdfs dfs -rm -R ${mw_out}
hdfs dfs -rm -R ${gt_user}
hdfs dfs -rm -R ${http_out}

HighSpeedUserAnalyseJob="cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob"

echo "------------------------x2_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $x2_table $bus_loc_mark_output ${x2_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------s1mme_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $s1mme_table $bus_loc_mark_output ${s1mme_new_output} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------gx_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $gx_table $bus_loc_mark_output ${gxrx_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------sv_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $sv_table $bus_loc_mark_output ${sv_xdr_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "-----------------------mw_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $mw_table $bus_loc_mark_output ${mw_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "-----------------------gtuser_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $mw_table $bus_loc_mark_output ${gt_user} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------mro_source_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $lte_mro_source $bus_loc_mark_output ${ltemrosource_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------sgs_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $sgs_table $bus_loc_mark_output ${sgs_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------uu_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $volteTrainAlalyse_business_input $bus_loc_mark_output ${uu_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}

echo "------------------------http_new_JOB------------------"
hadoop jar ${jar_file} $HighSpeedUserAnalyseJob $http_table $bus_loc_mark_output ${http_out} $public $process $grid $ht_sw_distance $distance_center ${ANALY_DATE} ${ANALY_HOUR}
wait

#checishibie
TRAINNUMBER="cn.com.dtmobile.hadoop.biz.train.job.numberiden.NumberIdenJob"
TRAINNUMBEROUTPUT="/datang2/output/trainiden/${ANALY_DATE}/${ANALY_HOUR}"

hdfs dfs  -rm -R ${TRAINNUMBEROUTPUT}
echo "--------------------TRAINNUMBER_JOB----------------------"
hadoop jar ${jar_file} ${TRAINNUMBER} \
${ltemrosource_out} \
${uu_out} \
${s1mme_new_output} \
${bus_volte_gt_busi_user} \
${trainstations} \
${trainstationtimes} \
${TRAINNUMBEROUTPUT}

range=10000
imsirange=8
step=5000
u1_output=${volteTrainAlalyse_output}/u1/${ANALY_DATE}/${ANALY_HOUR}
u2_1_output=${volteTrainAlalyse_output}/u2mapping/${ANALY_DATE}/${ANALY_HOUR}
u2_2_output=${volteTrainAlalyse_output}/u2/${ANALY_DATE}/${ANALY_HOUR}
u3_1_output=${volteTrainAlalyse_output}/u3mapping/${ANALY_DATE}/${ANALY_HOUR}
u3_2_output=${volteTrainAlalyse_output}/u3/${ANALY_DATE}/${ANALY_HOUR}

hdfs dfs -rm -R -skipTrash ${u1_output}
hdfs dfs -rm -R -skipTrash ${u2_1_output}
hdfs dfs -rm -R -skipTrash ${u2_2_output}
hdfs dfs -rm -R -skipTrash ${u3_1_output}
hdfs dfs -rm -R -skipTrash ${u3_2_output}

echo "------------------------U1_1_JOB------------------"
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU1_1Job ${uu_out} ${u1_output} $range $imsirange $step

echo "------------------------U2_1_JOB-------------------"
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_1Job ${u1_output} ${u2_1_output}

echo "------------------------U2_2_JOB--------------------"
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_2Job ${u1_output} ${u2_1_output}/u1ImsiMapping-r-00000 ${u2_2_output}

echo "------------------------U3_1_JOB------------------------"
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_1Job ${u2_2_output} ${u3_1_output}

echo "------------------------U3_2_JOB------------------------"
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_2Job ${u2_2_output} ${u3_1_output}/u2ImsiMapping-r-00000 ${u3_2_output}

#cellMR_main=cn.com.dtmobile.hadoop.biz.exception.job.CellMR
#mw=${xdr_new_output}/mw/${ANALY_DATE}/${ANALY_HOUR}
#mw_xdr=${mw}/tb*
#cellMR=/datang2/output/cellMR/${ANALY_DATE}/${ANALY_HOUR}
#hadoop fs -rm -R ${cellMR}

#echo "c------------------------CellMR------------------------"
#hadoop jar ${jar_file} ${cellMR_main} ${lte_mro_source} ${cellMR}
# user home
#user_home_main=cn.com.dtmobile.hadoop.biz.userHome.job.UserHomeJob
#user_home_out=${volteTrainAlalyse_output}/userHome/${ANALY_DATE}/${ANALY_HOUR}
#hdfs dfs -rm -R ${user_home_out}
#echo "------------------------user home------------------------"
#hadoop jar ${jar_file} \
#${user_home_main} \
#${volteTrainAlalyse_business_output}/VOLTE_GT_BUSI* \
#${volteTrainAlalyse_free_output}/VOLTE_GT_FREE*  \
#${xdr_new_output}/volte_gtuser_data/${ANALY_DATE}/${ANALY_HOUR}  \
#${user_home_out} \
#${phone_number}