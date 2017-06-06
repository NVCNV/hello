#!/bin/bash

for (( i=15;i<23;i++ ));do
./kpiAnaly.sh 20170105 ${i} shanxikpi2 ddl_for_kpi
echo "sh kpiAnaly.sh 20170105 ${i} shanxikpi2 ddl_for_kpi"
done
