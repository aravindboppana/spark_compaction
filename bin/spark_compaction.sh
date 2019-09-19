#!/usr/bin/env bash

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_DIR="${BIN_DIR}/../lib"
CONF_DIR="${BIN_DIR}/../conf"
JAR_FILE_LOCATION="${LIB_DIR}/spark-compaction-jar-with-dependencies.jar"
APPLICATION_CONF_FILE="${CONF_DIR}/application_configs.json"

COMPACTION_STRATEGY=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['compaction_strategy'];"`
SOURCE_DATA_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['hdfs']['source_data_location'];"`
SOURCE_DATA_BACKUP_LOCATION="${SOURCE_DATA_LOCATION}_backup"
TARGET_DATA_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['hdfs']['target_data_location'];"`

SOURCE_DB=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['source_db'];"`
SOURCE_TABLE=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['source_table'];"`
TARGET_DB=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['target_db'];"`
TARGET_TABLE=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['target_table'];"`

echo "BIN_DIR: ${BIN_DIR}"
echo "LIB_DIR: ${LIB_DIR}"
echo "JAR_FILE_LOCATION: ${JAR_FILE_LOCATION}"
echo "APPLICATION_CONF_FILE: ${APPLICATION_CONF_FILE}"
echo "COMPACTION STRATEGY: " ${COMPACTION_STRATEGY}

compaction() {

    APP_NAME=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['app_name'];"`
    SPARK_MASTER=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['master'];"`

    echo "APP_NAME: ${APP_NAME}"

    echo "Launching Spark Streaming Application in Yarn Client Mode"
    SPARK_SUBMIT_STARTUP_CMD="spark2-submit --master yarn --deploy-mode client --driver-class-path ${CONF_DIR}:${JAR_FILE_LOCATION} --class com.clairvoyant.nyu.bigdata.spark.SparkCompaction ${JAR_FILE_LOCATION} "


    echo "executing: ${SPARK_SUBMIT_STARTUP_CMD}"
    eval ${SPARK_SUBMIT_STARTUP_CMD}

}

echo "Starting Compaction"

compaction

if [[ $? = 0 && "${COMPACTION_STRATEGY}" = "rewrite" ]]; then
    echo "BACKING UP THE SOURCE DATA BEFORE COMPACTION"
    hadoop fs -cp ${SOURCE_DATA_LOCATION} ${SOURCE_DATA_BACKUP_LOCATION}
    echo "Dropping Source Data"
    hadoop fs -rm -r ${SOURCE_DATA_LOCATION}
    echo "Copying Compacted files to Source Location"
    hadoop fs -cp /tmp/Spark_Compaction ${SOURCE_DATA_LOCATION}
    if [[ $? = 0 ]]; then
        echo "Dropping Compacted files in Temp location"
        hadop fs -rm -r /tmp/Spark_Compaction
        echo "Dropping Source Data Backup before compacting"
        hadoop fs -rm -r ${SOURCE_DATA_BACKUP_LOCATION}
    fi
    impala-shell -q "INVALIDATE METADATA ${SOURCE_DB}.${SOURCE_TABLE}"
    impala-shell -q "COMPUTE STATS ${SOURCE_DB}.${SOURCE_TABLE}"
fi

if [[ $? = 0 && "${COMPACTION_STRATEGY}" = "new" ]]; then
    echo "Create new external table as the one in the Source DB"
    hive -e "CREATE EXTERNAL TABLE ${TARGET_DB}.${TARGET_TABLE} LIKE ${SOURCE_DB}.${SOURCE_TABLE} STORED AS PARQUET LOCATION '${TARGET_DATA_LOCATION}' "
    impala-shell -q "INVALIDATE METADATA ${TARGET_DB}.${TARGET_TABLE}"
    impala-shell -q "COMPUTE STATS ${TARGET_DB}.${TARGET_TABLE}"

fi

if [[ $? = 0 ]]; then
    echo "SUCCESSFULLY COMPLETED COMPACTION"
fi

