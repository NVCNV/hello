#!/bin/bash
# Run it every hour.
my_path="$(cd "$(dirname "$0")"; pwd)"
cd $my_path
jar_file="/dt/lib/dt_mobile.jar"

# date
ANALY_DATE=$1
#ANALY_HOUR=$2


# parameters
process="/datang2/parameter/profess.txt"
ht_switch="/datang2/parameter/ht_switch.txt"
public="/datang2/parameter/public.txt"
ht_sw_distance="/datang2/parameter/ht_sw_distance.csv"
grid="/datang2/parameter/grid.csv"
exceptionMap="/datang2/parameter/EXCEPTIONMAP.tsv"
ltecell="/datang2/parameter/ltecell.txt"
t_process="/datang2/parameter/profess.txt"
process_updown="/datang2/parameter/cellID_process.csv"
phone_number="/datang2/parameter/phone_number.csv"


volteTrainAlalyse_output="/datang2/output"

xdr_new_output="/datang2/output/xdrnew"

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
local_path_upordown=/home/hadoop/upordown_all

s1mme_new_output=${xdr_new_output}/s1mme/${ANALY_DATE}
u2_output=${volteTrainAlalyse_output}/u2/${ANALY_DATE}
upordown_for_checi=${volteTrainAlalyse_output}/upordown.csv

hdfs dfs -rm -R -skipTrash ${u3_u2}
hdfs dfs -mkdir -p ${u3_u2}
hdfs dfs -rm -R -skipTrash ${s1mme_allday}
hdfs dfs -mkdir -p ${s1mme_allday}
hdfs dfs -rm -R -skipTrash ${u2_allday}
hdfs dfs -mkdir -p ${u2_allday}
hdfs fs -rm -R -skipTrash ${u4_2_output}

rm -rf ${local_path_ralation_u2_u3}
mkdir -p ${local_path_ralation_u2_u3}

for (( i=0;i<=23;i++  ))
do
if [ $i -lt 10 ]; then
j=`printf "%02d" "$i"`
else
j=$i
fi

# process data of s1mme whole day.
file_names_s1=`hdfs dfs -ls ${s1mme_new_output}/${j}|awk '{print $8}'`

for name in ${file_names_s1}
do
hdfs dfs -mv ${name} ${name}${j}
echo "hdfs dfs -mv ${name} ${name}${j}"
done

hdfs dfs -cp ${s1mme_new_output}/${j}/*  ${s1mme_allday}
echo "hdfs dfs -cp ${s1mme_new_output}/${j}/*  ${s1mme_allday}"
# process data of u2 whole day.

file_names_u2=`hdfs dfs -ls ${u2_output}/${j}|awk '{print $8}'`

for name in ${file_names_u2}
do
hdfs dfs -mv ${name} ${name}${j}
echo "hdfs dfs -mv ${name} ${name}${j}"
done

hdfs dfs -cp ${u2_output}/${j}/* ${u2_allday}
echo "hdfs dfs -cp ${u2_output}/${j}/* ${u2_allday}"
hdfs dfs -get ${u3_1_output}/${j}/u2ImsiMapping* ${local_path_ralation_u2_u3}/u2Mapping${j}
done

cat ${local_path_ralation_u2_u3}/* >> ${local_path_ralation_u2_u3}/u2u3relation.csv
hdfs dfs -put ${local_path_ralation_u2_u3}/u2u3relation.csv ${u3_u2}/u2u3relation.csv
imsirange=10


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
${imsirange} \
${u4_1_output}

hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU4_2Job ${u3_2_output}/00 \
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
${u4_1_output}/u3ImsiMapping-r-00000 \
${u4_2_output}

# UpOrDown
updowntrain_ouput=/datang2/output/updowntrain/${ANALY_DATE}
reducers=15
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
${second_t} \
${reducers}

rm -rf ${local_path_upordown}
rm -rf -skipTrash ${upordown_for_checi}
hdfs dfs -get ${updowntrain_ouput} ${local_path_upordown}
cat ${local_path_upordown}/* > /home/hadoop/upordown.csv
hdfs dfs -put /home/hadoop/upordown.csv ${upordown_for_checi}  

# number identified
trainstations=/datang2/parameter/stations.txt
trainstationtimes=/datang2/parameter/stationTimes.txt
size=10
trainiden_output=/datang2/output/trainiden/${ANALY_DATE}
hdfs dfs -rm -R ${trainiden_output}
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.numberiden.NumberIdenJob ${u4_2_output}/u4* ${upordown_for_checi} ${trainstations} $trainstationtimes $size ${trainiden_output}

