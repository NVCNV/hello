#!/bin/bash
# Run it every hour.
my_path="$(cd "$(dirname "$0")"; pwd)"
cd $my_path
jar_file="/dt/lib/dt_mobile.jar"

# date
ANALY_DATE=$1
#ANALY_HOUR=$2


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


volteTrainAlalyse_output="/datang/output"
bus_volte_gt_busi_user="${volteTrainAlalyse_business_output}/VOLTE_GT_BUSI_USER-r-00000"
bus_volte_common_user="${volteTrainAlalyse_business_output}/COMM_USER_DATA*"
bus_loc_mark_output="${volteTrainAlalyse_business_output}/loc_guser_mark*"


xdr_new_output="/datang/output/xdrnew"
s1mme_new_output=${xdr_new_output}/s1mme/${ANALY_DATE}
u2_output=${volteTrainAlalyse_output}/u2/${ANALY_DATE}


range=10000
imsirange=8
step=5000
u3_1_output=${volteTrainAlalyse_output}/u3mapping/${ANALY_DATE}
u3_2_output=${volteTrainAlalyse_output}/u3/${ANALY_DATE}
u4_1_output=${volteTrainAlalyse_output}/u4mapping/${ANALY_DATE}
u4_2_output=${volteTrainAlalyse_output}/u4/${ANALY_DATE}

u3_u2=${volteTrainAlalyse_output}/u3u2Relation/${ANALY_DATE}
s1mme_allday=${volteTrainAlalyse_output}/s1mme_allday/${ANALY_DATE}
u2_allday=${volteTrainAlalyse_output}/u2_allday/${ANALY_DATE}
local_path_ralation_u2_u3=/home/hadoop/relationOfu23
local_path_s1mme=/home/sftp/s1mme_allday
local_path_u2=/home/sftp/u2_allday



hdfs dfs -rm -R -skipTrash ${u3_u2}
hdfs dfs -mkdir -p ${u3_u2}
hdfs dfs -rm -R -skipTrash ${s1mme_allday}
hdfs dfs -mkdir -p ${s1mme_allday}
hdfs dfs -rm -R -skipTrash ${u2_allday}
hdfs dfs -mkdir -p ${u2_allday}

rm -rf ${local_path_s1mme}
mkdir -p ${local_path_s1mme}
rm -rf ${local_path_ralation_u2_u3}
mkdir -p ${local_path_ralation_u2_u3}
rm -rf ${local_path_u2}
mkdir -p ${local_path_u2}

for (( i=0;i<=23;i++  ))
do
if [ $i -lt 10 ]; then
j=`printf "%02d" "$i"`
else
j=$i
fi

#mkdir -p ${local_path_s1mme}/s1mme${j}
#hdfs dfs -get ${s1mme_new_output}/${j}/* ${local_path_s1mme}/s1mme${j}
#cat ${local_path_s1mme}/s1mme${j}/* >> ${local_path_s1mme}/s1mme_allday${j}.csv
#hdfs dfs -put ${local_path_s1mme}/s1mme_allday${j}.csv ${s1mme_allday}

#mkdir -p ${local_path_u2}/u2${j}
#hdfs dfs -get ${u2_output}/${j}/* ${local_path_u2}/u2${j}
#cat ${local_path_u2}/u2${j}/* >> ${local_path_u2}/u2_allday${j}.csv
#hdfs dfs -put ${local_path_u2}/u2_allday${j}.csv ${u2_allday}


hdfs dfs -get ${u3_1_output}/${j}/u2ImsiMapping* ${local_path_ralation_u2_u3}/u2Mapping${j}
done

cat ${local_path_ralation_u2_u3}/* >> ${local_path_ralation_u2_u3}/u2u3relation.csv
hdfs dfs -put ${local_path_ralation_u2_u3}/u2u3relation.csv ${u3_u2}/u2u3relation.csv

hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU4_1Job ${u3_2_output}/00 \
${u3_2_output}/01 \
${u3_2_output}/02 \
${u3_2_output}/03 \
${u3_2_output}/04 \
${u3_2_output}/05 \
${u3_2_output}/06 \
${u3_2_output}/07 \
${u3_2_output}/08 \
${u3_2_output}/09 \
${u3_2_output}/10 \
${u3_2_output}/11 \
${u3_2_output}/12 \
${u3_2_output}/13 \
${u3_2_output}/14 \
${u3_2_output}/15 \
${u3_2_output}/16 \
${u3_2_output}/17 \
${u3_2_output}/18 \
${u3_2_output}/19 \
${u3_2_output}/20 \
${u3_2_output}/21 \
${u3_2_output}/22 \
${u3_2_output}/23 \
${u4_1_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU4_2Job ${u3_2_output}/00 ${u3_2_output}/01 ${u3_2_output}/02 ${u3_2_output}/03 ${u3_2_output}/04 ${u3_2_output}/05 ${u3_2_output}/06 ${u3_2_output}/07 ${u3_2_output}/08 ${u3_2_output}/09 ${u3_2_output}/10 ${u3_2_output}/11 ${u3_2_output}/12 ${u3_2_output}/13 ${u3_2_output}/14 ${u3_2_output}/15 ${u3_2_output}/16 ${u3_2_output}/17 ${u3_2_output}/18 ${u3_2_output}/19 ${u3_2_output}/20 ${u3_2_output}/21 ${u3_2_output}/22 ${u3_2_output}/23 ${u4_1_output}/u3ImsiMapping-r-00000 ${u4_2_output}

# UpOrDown
updowntrain_ouput=/datang/output/updowntrain/${ANALY_DATE}

second_t=100
hadoop fs -rm -r $updowntrain_ouput
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.upordown.job.UpDownAnalysisJob \
${u2_allday} \
${s1mme_allday} \
${updowntrain_ouput} \
${process_updown} \
${u4_1_output}/u3ImsiMapping-r-00000 \
${u4_2_output}/u3Mapping-r-00000 \
${u3_u2}/u2u3relation.csv \
${second_t}

# number identified
trainstations=/datang/parameter/stations.txt
trainstationtimes=/datang/parameter/stationTimes.txt
size=10
trainiden_output=/datang/output/trainiden/${ANALY_DATE}
hdfs dfs -rm -R ${trainiden_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.numberiden.NumberIdenJob ${u4_2_output} $updowntrain_ouput/part-r-00000 ${trainstations} $trainstationtimes $size ${trainiden_output}



