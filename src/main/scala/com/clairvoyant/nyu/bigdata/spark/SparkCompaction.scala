package com.clairvoyant.nyu.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object SparkCompaction {

    def main(args: Array[String]): Unit = {

        val LOGGER: Logger = LoggerFactory.getLogger(SparkCompaction.getClass.getName)

        // Load values form the Config file(application.json)
        val config: Config = ConfigFactory.load("application_configs.json")

        val SPARK_APP_NAME: String = config.getString("spark.app_name")
        val SPARK_MASTER: String = config.getString("spark.master")

        val ENABLE_NUM_FILES = config.getBoolean("compaction.enable_num_files")
        var NUM_FILES = config.getInt("compaction.num_files")
        val COMPRESSION = config.getString("compaction.compression")
        val SIZE_RANGES_FOR_COMPACTION: ConfigList = config.getList("compaction.size_ranges_for_compaction")
        val DECODED_SIZE_RANGES_FOR_COMPACTION: Array[AnyRef] = SIZE_RANGES_FOR_COMPACTION.unwrapped().toArray

        val COMPACTION_STRATEGY = args(0)
        val SOURCE_DATA_LOCATION_HDFS = args(1)
        var TARGET_DATA_LOCATION_HDFS = "/tmp"

        if(args.length < 2){
            LOGGER.error("Provide enough arguments to process")
            System.exit(0)
        } else if(COMPACTION_STRATEGY == "new" && args.length == 3){
            TARGET_DATA_LOCATION_HDFS = args(2)
        } else if(COMPACTION_STRATEGY == "new" && args.length < 3){
            LOGGER.error("Provide Source and target data locations as arguments")
            System.exit(0)
        }

        LOGGER.info("SPARK_APP_NAME: " + SPARK_APP_NAME)
        LOGGER.info("SPARK_MASTER: " + SPARK_MASTER)

        LOGGER.info("SOURCE_DATA_LOCATION_HDFS: " + SOURCE_DATA_LOCATION_HDFS)
        LOGGER.info("TARGET_DATA_LOCATION_HDFS: " + TARGET_DATA_LOCATION_HDFS)

        LOGGER.info("COMPACTION_STRATEGY: " + COMPACTION_STRATEGY)
        LOGGER.info("ENABLE_NUM_FILES: " + ENABLE_NUM_FILES)
        LOGGER.info("NUM_FILES: " + NUM_FILES)
        LOGGER.info("COMPRESSION: " + COMPRESSION)

        val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()

        if (!ENABLE_NUM_FILES) {

            val hdfs: FileSystem = FileSystem.get(new Configuration())

            def roundUp(d: Double) = math.ceil(d).toInt

            val hadoopPath = new Path(SOURCE_DATA_LOCATION_HDFS)

            val recursive = false
            val ri: RemoteIterator[LocatedFileStatus] = hdfs.listFiles(hadoopPath, recursive)
            val it: Iterator[LocatedFileStatus] = new Iterator[LocatedFileStatus]() {
                override def hasNext: Boolean = ri.hasNext

                override def next(): LocatedFileStatus = ri.next()
            }

            var partition_size = 256

            // Materialize iterator
            val files = it.toList
            LOGGER.info("No.of files: " + files.size)

            val hdfs_dir_size_in_mb = files.map(_.getLen).sum * 0.00000095367432
            LOGGER.info("Size: " + hdfs_dir_size_in_mb + " MB")

            val hdfs_dir_size_in_gb = hdfs_dir_size_in_mb * 0.0009756
            LOGGER.info("Size in GB: " + hdfs_dir_size_in_gb)

            DECODED_SIZE_RANGES_FOR_COMPACTION.foreach(f = map => {
                val hashMap = map.asInstanceOf[java.util.HashMap[String, Int]]

                val min_size_in_gb = hashMap.get("min_size_in_gb")
                var max_size_in_gb = hashMap.get("max_size_in_gb")

                if(max_size_in_gb == 0){
                    max_size_in_gb = hdfs_dir_size_in_gb.toInt
                }

                if ((min_size_in_gb <= hdfs_dir_size_in_gb) && (max_size_in_gb >= hdfs_dir_size_in_gb)) {
                    partition_size = hashMap.get("size_after_compaction_in_mb")
                }

            })

            NUM_FILES = roundUp(hdfs_dir_size_in_mb / partition_size)
        }
        LOGGER.info("Number of Output Files: " + NUM_FILES)

        val df = spark.read.parquet(SOURCE_DATA_LOCATION_HDFS)

        if(COMPACTION_STRATEGY == "overwrite"){
            LOGGER.info("Overwriting Strategy")
            df.coalesce(NUM_FILES).write.mode("overwrite").parquet(SOURCE_DATA_LOCATION_HDFS + "_temp")
        }
        else {
            LOGGER.info("Writing to new Location")
            df.coalesce(NUM_FILES).write.parquet(TARGET_DATA_LOCATION_HDFS)
        }

    }

}
