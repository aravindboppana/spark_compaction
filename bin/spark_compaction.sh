#!/usr/bin/env bash

HELP_STR="""

Please provide the arguments as follows

Example: sh spark_compaction.sh {source_db_name} {source_table_name} {target_db_name} {target_table_name}

"""

SOURCE_DB_NAME="$1"
SOURCE_TABLE_NAME="$2"
TARGET_DB_NAME="$3"
TARGET_TABLE_NAME="$4"

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_DIR="${BIN_DIR}/../lib"
CONF_DIR="${BIN_DIR}/../conf"
JAR_FILE_LOCATION="${LIB_DIR}/spark-compaction-jar-with-dependencies.jar"
APPLICATION_CONF_FILE="${CONF_DIR}/application_configs.json"

COMPACTION_STRATEGY=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['compaction']['compaction_strategy'];"`
SOURCE_DATA_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['hdfs']['source_data_location'];"`
SOURCE_DATA_BACKUP_LOCATION="${SOURCE_DATA_LOCATION}_backup"
TARGET_DATA_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['hdfs']['target_data_location'];"`

if [[ -z "${SOURCE_DB_NAME}" || -z "${SOURCE_TABLE_NAME}" ]]; then
    echo "Please provide all the arguments to proceed with the compaction Job. Please provide the arguments as follows"
    echo "${HELP_STR}"
    exit 0
fi

impala-shell -q "SELECT * FROM ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME} LIMIT 1"
if [[ "$?=0" ]]; then
    echo "Source Database and table exists. Proceeding with the next task"
else
    echo "Source Database or Table doesn't exist. Exiting the process"
    exit 1
fi

echo "BIN_DIR: ${BIN_DIR}"
echo "LIB_DIR: ${LIB_DIR}"
echo "JAR_FILE_LOCATION: ${JAR_FILE_LOCATION}"
echo "APPLICATION_CONF_FILE: ${APPLICATION_CONF_FILE}"
echo "COMPACTION STRATEGY: " ${COMPACTION_STRATEGY}
echo "SOURCE DB: " ${SOURCE_DB_NAME}
echo "SOURCE TABLE: " ${SOURCE_TABLE_NAME}

if [[ "${COMPACTION_STRATEGY}" == "new" ]]; then
    if [[ -z "${TARGET_DB_NAME}" || -z "${TARGET_TABLE_NAME}" ]]; then
        echo "Please provide all the arguments to proceed with the compaction Job. Please provide the arguments as follows"
        echo "${HELP_STR}"
        exit 0
    fi
    echo "TARGET DB: " ${TARGET_DB_NAME}
    echo "TARGET TABLE: " ${TARGET_TABLE_NAME}

    impala-shell -q "SELECT * FROM ${TARGET_DB_NAME}.${TARGET_TABLE_NAME} LIMIT 1"
    if [[ "$?=0" ]]; then
        echo "Target table already exists. Exiting the application"
        exit 1
    else
        echo "Proceeding with the next task"
    fi
fi

compaction() {

    APP_NAME=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['app_name'];"`
    SPARK_MASTER=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['master'];"`

    echo "APP_NAME: ${APP_NAME}"

    echo "Launching Spark Streaming Application in Yarn Client Mode"
    SPARK_SUBMIT_STARTUP_CMD="spark2-submit --master yarn --deploy-mode client --driver-class-path ${CONF_DIR}:${JAR_FILE_LOCATION} --class com.clairvoyant.nyu.bigdata.spark.SparkCompaction ${JAR_FILE_LOCATION} "


    echo "executing: ${SPARK_SUBMIT_STARTUP_CMD}"
    eval ${SPARK_SUBMIT_STARTUP_CMD}

}

invalidate_metadata_and_compute_stats() {

    DATABASE_NAME="$1"
    TABLE_NAME="$2"

    impala-shell -q "INVALIDATE METADATA ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "COMPUTE STATS ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "DESCRIBE EXTENDED ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "SHOW TABLE STATS ${DATABASE_NAME}.${TABLE_NAME}"
    impala-shell -q "SELECT COUNT(1) FROM ${DATABASE_NAME}.${TABLE_NAME}"

}


echo "Starting Compaction"

compaction
# Revisit Saving to temp location in HDFS after figuring out the solution for Spark out of memory
if [[ $? = 0 && "${COMPACTION_STRATEGY}" = "rewrite" ]]; then
    echo "BACKING UP THE SOURCE DATA BEFORE COMPACTION"
    hadoop fs -cp ${SOURCE_DATA_LOCATION} ${SOURCE_DATA_BACKUP_LOCATION}
    echo "Dropping Source Data"
    hadoop fs -rm -R -skipTrash ${SOURCE_DATA_LOCATION}
    echo "Copying Compacted files to Source Location"
    hadoop fs -cp /tmp/Spark_Compaction ${SOURCE_DATA_LOCATION}
    if [[ $? = 0 ]]; then
        echo "Dropping Compacted files in Temp location"
        hadoop fs -rm -R -skipTrash /tmp/Spark_Compaction
        echo "Dropping Source Data Backup before compacting"
        hadoop fs -rm -R -skipTrash ${SOURCE_DATA_BACKUP_LOCATION}
    fi
    invalidate_metadata_and_compute_stats "${SOURCE_DB_NAME} ${SOURCE_TABLE_NAME}"
fi

if [[ $? = 0 && "${COMPACTION_STRATEGY}" = "new" ]]; then
    echo "Create new external table as the one in the Source DB"
    impala -q "CREATE EXTERNAL TABLE ${TARGET_DB_NAME}.${TARGET_TABLE_NAME} LIKE ${SOURCE_DB_NAME}.${SOURCE_TABLE_NAME} STORED AS PARQUET LOCATION '${TARGET_DATA_LOCATION}' "
    invalidate_metadata_and_compute_stats "${TARGET_DB_NAME} ${TARGET_TABLE_NAME}"

fi

if [[ $? = 0 ]]; then
    echo "SUCCESSFULLY COMPLETED COMPACTION"
fi

