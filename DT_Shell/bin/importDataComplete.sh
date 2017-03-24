#!/bin/bash
ANALY_DATE=`date +%Y%m%d`
ANALY_HOUR="`date -d ' -0 hour' +%H`"
rm -rf /dt/tmp/*
touch /dt/tmp/$ANALY_DATE$ANALY_HOUR