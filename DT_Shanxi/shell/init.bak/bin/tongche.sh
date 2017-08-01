#!/usr/bin/env bash
range=10000
imsirange=8
step=5000
u1_output=$volteTrainAlalyse_output/u1
u2_1_output=$volteTrainAlalyse_output/u2mapping
u2_2_output=$volteTrainAlalyse_output/u2
u3_1_output=$volteTrainAlalyse_output/u3mapping
u3_2_output=$volteTrainAlalyse_output/u3
u4_1_output=$volteTrainAlalyse_output/u4mapping
u4_2_output=$volteTrainAlalyse_output/u4
hadoop fs -rm -r $u1_output
hadoop fs -rm -r $u2_1_output
hadoop fs -rm -r $u2_2_output
hadoop fs -rm -r $u3_1_output
hadoop fs -rm -r $u3_2_output
hadoop fs -rm -r $u4_1_output
hadoop fs -rm -r $u4_2_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU1_1Job $UU_new���·�� $u1_output $range $imsirange $step
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_1Job $u1_output $u2_1_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU2_2Job $u1_output $u2_1_output/u1ImsiMapping-r-00000 $u2_2_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_1Job $u2_2_output $u3_1_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU3_2Job $u2_2_output $u3_1_output/u2Mapping-r-00000 $u3_2_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU4_1Job 24Сʱu3���� $u4_1_output
hadoop jar ${jar_file} cn.com.dtmobile.hadoop.biz.train.job.trainsame.TrainSameU4_2Job $u3_2_output 24Сʱu3���� $u4_1_output $u4_2_output

