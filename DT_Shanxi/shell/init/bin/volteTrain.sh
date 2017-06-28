#!/bin/bash
# Run it every hour.
my_path="$(cd "$(dirname "$0")"; pwd)"
cd $my_path
jar_file="/dt/lib/dt_mobile.jar"

# date
ANALY_DATE=$1
ANALY_HOUR=$2

# free
user_distinguish_S1mmeXdr="cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.VolteTrainS1mmeJob"
# busi
user_distinguish_uuXdr="cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.VolteTrainUuJob"

# parameters
process="/datang/parameter/profess.txt"
ht_switch="/datang/parameter/ht_switch.txt"
public="/datang/parameter/public.txt"
ht_sw_distance="/datang/parameter/ht_sw_distance.csv"
grid="/datang/parameter/grid.csv"
exceptionMap="/datang/parameter/EXCEPTIONMAP.tsv"
ltecell="/datang/parameter/ltecell.txt"
t_process="/datang/parameter/profess.txt"
process_updown="/datang/parameter/cellID_process.csv"
phone_number="/datang/parameter/phone_number.csv"

# high-speed rail 
confSpeed=170
distance_center=50

volteTrainAlalyse_business_input="/datang/TB_XDR_IFC_UU/${ANALY_DATE}/${ANALY_HOUR}/*"
volteTrainAlalyse_free_input="/datang/TB_XDR_IFC_S1MME/${ANALY_DATE}/${ANALY_HOUR}/*"

volteTrainAlalyse_output="/datang/output"
volteTrainAlalyse_business_output="${volteTrainAlalyse_output}/business/${ANALY_DATE}/${ANALY_HOUR}"
volteTrainAlalyse_free_output="${volteTrainAlalyse_output}/free/${ANALY_DATE}/${ANALY_HOUR}"
bus_volte_gt_busi_user="${volteTrainAlalyse_business_output}/VOLTE_GT_BUSI_USER-r-00000"
bus_volte_common_user="${volteTrainAlalyse_business_output}/COMM_USER_DATA*"
bus_loc_mark_output="${volteTrainAlalyse_business_output}/loc_guser_mark*"

#free_volte_gt_busi_user="${volteTrainAlalyse_free}/VOLTE_GT_BUSI_USER*"
#free_volte_common_user="${volteTrainAlalyse_free}/COMM_USER_DATA*"
#free_loc_mark_output="${volteTrainAlalyse_free}/loc_guser_mark*"
hadoop fs -rm -r ${volteTrainAlalyse_business_output}
echo "business table"
hadoop jar ${jar_file} ${user_distinguish_uuXdr} ${volteTrainAlalyse_business_input} ${volteTrainAlalyse_business_output} ${process} ${public} ${ht_switch} ${ht_sw_distance} ${confSpeed}

x2_table="/datang/TB_XDR_IFC_X2/${ANALY_DATE}/${ANALY_HOUR}"
s1mme_table="/datang/TB_XDR_IFC_S1MME/${ANALY_DATE}/${ANALY_HOUR}"
gx_table="/datang/TB_XDR_IFC_GXRX/${ANALY_DATE}/${ANALY_HOUR}"
sv_table="/datang/TB_XDR_IFC_SV/${ANALY_DATE}/${ANALY_HOUR}"
mw_table="/datang/TB_XDR_IFC_GMMWMGMIMJISC/${ANALY_DATE}/${ANALY_HOUR}"
lte_mro_source="/datang/LTE_MRO_SOURCE/${ANALY_DATE}/${ANALY_HOUR}"
sgs_table="/datang/TB_XDR_IFC_SGS/${ANALY_DATE}/${ANALY_HOUR}"
xdr_new_output="/datang/output/xdrnew"

echo "free table"
hadoop fs -rm -r $volteTrainAlalyse_free_output
hadoop jar ${jar_file} ${user_distinguish_S1mmeXdr} ${volteTrainAlalyse_free_input} ${volteTrainAlalyse_free_output} ${process} ${public} ${bus_volte_gt_busi_user} ${confSpeed}

#hadoop fs -rm -r ${xdr_new_output}
hadoop fs -rm -r ${xdr_new_output}/ltemrosource
echo "eight new tables"

x2_out=${xdr_new_output}/x2/${ANALY_DATE}/${ANALY_HOUR}
gxrx_out=${xdr_new_output}/gxrx/${ANALY_DATE}/${ANALY_HOUR}
sv_xdr_out=${xdr_new_output}/sv_xdr/${ANALY_DATE}/${ANALY_HOUR}
ltemrosource_out=${xdr_new_output}/ltemrosource/${ANALY_DATE}/${ANALY_HOUR}
sgs_out=${xdr_new_output}/sgs/${ANALY_DATE}/${ANALY_HOUR}
uu_out=${xdr_new_output}/uu/${ANALY_DATE}/${ANALY_HOUR}
s1mme_new_output=${xdr_new_output}/s1mme/${ANALY_DATE}/${ANALY_HOUR}
mw_out=${xdr_new_output}/mw/${ANALY_DATE}/${ANALY_HOUR}

hdfs dfs -rm -R ${x2_out}
hdfs dfs -rm -R ${gxrx_out}
hdfs dfs -rm -R ${sv_xdr_out}
hdfs dfs -rm -R ${ltemrosource_out}
hdfs dfs -rm -R ${s1mme_new_output}
hdfs dfs -rm -R ${sgs_out}
hdfs dfs -rm -R ${uu_out}

hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $x2_table $bus_loc_mark_output ${x2_out} $public $process $grid $ht_sw_distance $distance_center  &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $gx_table $bus_loc_mark_output ${gxrx_out} $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $sv_table $bus_loc_mark_output ${sv_xdr_out} $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $mw_table $bus_loc_mark_output ${mw_out} $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $s1mme_table $bus_loc_mark_output $s1mme_new_output $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $lte_mro_source $bus_loc_mark_output ${ltemrosource_out} $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $sgs_table $bus_loc_mark_output ${sgs_out} $public $process $grid $ht_sw_distance $distance_center &
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.highspeeduser.HighSpeedUserAnalyseJob $volteTrainAlalyse_business_input $bus_loc_mark_output ${uu_out} $public $process $grid $ht_sw_distance $distance_center &

wait

range=10000
imsirange=8
step=5000
u1_output=${volteTrainAlalyse_output}/u1/${ANALY_DATE}/${ANALY_HOUR}
u2_1_output=${volteTrainAlalyse_output}/u2mapping/${ANALY_DATE}/${ANALY_HOUR}
u2_2_output=${volteTrainAlalyse_output}/u2/${ANALY_DATE}/${ANALY_HOUR}
u3_1_output=${volteTrainAlalyse_output}/u3mapping/${ANALY_DATE}/${ANALY_HOUR}
u3_2_output=${volteTrainAlalyse_output}/u3/${ANALY_DATE}
u4_1_output=${volteTrainAlalyse_output}/u4mapping/${ANALY_DATE}/${ANALY_HOUR}
u4_2_output=${volteTrainAlalyse_output}/u4/${ANALY_DATE}/${ANALY_HOUR}


hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU1_1Job ${uu_out} ${u1_output} $range $imsirange $step

hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_1Job ${u1_output} ${u2_1_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_2Job ${u1_output} ${u2_1_output}/u1ImsiMapping-r-00000 ${u2_2_output}

hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_1Job ${u2_2_output} ${u3_1_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_2Job ${u2_2_output} ${u3_1_output}/u2ImsiMapping-r-00000 ${u3_2_output}/${ANALY_HOUR}

# number identified
trainstations=/datang/parameter/stations.txt
trainstationtimes=/datang/parameter/stationTimes.txt
size=10
trainiden_output=/datang/output/trainiden/${ANALY_DATE}/${ANALY_HOUR}
#hdfs dfs -rm -R ${trainiden_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.numberiden.NumberIdenJob ${u4_2_output} $updowntrain_ouput/part-r-00000 ${trainstations} $trainstationtimes $size $trainiden_output

cellMR_main=cn.com.dtmobile.hadoop.biz.exception.job.CellMR
mw=${xdr_new_output}/mw/${ANALY_DATE}/${ANALY_HOUR}
mw_xdr=${mw}/tb*
cellMR=/datang/cellMR/${ANALY_DATE}/${ANALY_HOUR}
#hadoop fs -rm -R ${cellMR}
hadoop jar ${jar_file} ${cellMR_main} ${lte_mro_source} ${cellMR}

# user home
user_home_main=cn.com.dtmobile.hadoop.biz.userHome.job.UserHomeJob
user_home_out=${volteTrainAlalyse_output}/userHome/${ANALY_DATE}/${ANALY_HOUR}
hdfs dfs -rm -R ${user_home_out}
hadoop jar ${jar_file} \
${user_home_main} \
${volteTrainAlalyse_business_output}/VOLTE_GT_BUSI* \
${volteTrainAlalyse_free_output}/VOLTE_GT_FREE*  \
${mw}/volte*  \
${user_home_out} \
${phone_number}






