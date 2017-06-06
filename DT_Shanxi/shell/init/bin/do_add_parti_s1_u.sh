#!/bin/bash

for (( i=0;i<=23;i++ ))
do
if [ $i -lt 10 ]; then
j=`printf "%02d" "$i"`
else
j=$i
fi

./add_partition_for_s1_u.sh 20170105 ${j}  datang
done
