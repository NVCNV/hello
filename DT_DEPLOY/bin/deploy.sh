#!/bin/bash
source /etc/profile

PROJECT_NAME=liaoning
DEPLOY_PATH=/app/tmp/dtmobile/DT_Analy
BACKUP_PATH=/app/tmp/dtmobile/backup
PROJECT_PATH=/opt/DT_Analy_PATH/DT_Analy

build_project()
{
    if [ ! -d "${DEPLOY_PATH}/jars/" ];then
    mkdir -p ${DEPLOY_PATH}/jars
    fi
    if [ ! -d "${DEPLOY_PATH}/bin/" ];then
    mkdir -p ${DEPLOY_PATH}/bin
    fi
    if [ ! -d "${DEPLOY_PATH}/conf/" ];then
    mkdir -p ${DEPLOY_PATH}/conf
    fi

    cd ${PROJECT_PATH}

    echo ".......building project start......"

    mvn package -Dproject.name=${PROJECT_NAME} -Ddeploy.path=${DEPLOY_PATH} -P ${PROJECT_NAME} -D skipTests
    sleep 3

    ls

    echo "......copying DT_Core.jar to ${DEPLOY_PATH}......"
    cp ./DT_Core/target/*.jar ${DEPLOY_PATH}/jars/

    echo "......copying DT_Spark.jar to ${DEPLOY_PATH}......"
    cp ./DT_Spark/target/*.jar ${DEPLOY_PATH}/jars/

    echo "......copying DT_Hadoop.jar to ${DEPLOY_PATH}......"
    cp ./DT_Hadoop/target/*.jar ${DEPLOY_PATH}/jars/

    echo "......copying DT_Liaoning.jar to ${DEPLOY_PATH}......"
    cp ./DT_Liaoning/target/*.jar ${DEPLOY_PATH}/jars/

    echo "......copying DT_Shanxi.jar to ${DEPLOY_PATH}......"
    cp ./DT_Shanxi/target/*.jar ${DEPLOY_PATH}/jars/

    echo "......building project end......"
}

deploy_backup()
{
    if [ ! -d "${BACKUP_PATH}/" ];then
    mkdir -p ${BACKUP_PATH}
    fi
    cd ${DEPLOY_PATH}/
    cd ../
    tar zcvf dt_analy_`date +%y%m%d%H%M%S`.tar.gz DT_Analy
    sleep 3
    ls
    mv *.tar.gz ${BACKUP_PATH}/
    rm -rf ${DEPLOY_PATH}/*
}

deploy_backup
build_project