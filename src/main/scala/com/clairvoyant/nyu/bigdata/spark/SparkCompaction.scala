package com.clairvoyant.nyu.bigdata.spark

import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkCompaction {

    def main(args: Array[String]): Unit = {

        // Load values form the Config file(application.json)
        val config: Config = ConfigFactory.load("application_configs.json")

        val SPARK_APP_NAME: String = config.getString("spark.app_name")
        val SPARK_MASTER: String = config.getString("spark.master")

        val SOURCE_DATA_LOCATION_HDFS = config.getString("hdfs.source_data_location")
        val TARGET_DATA_LOCATION_HDFS = config.getString("hdfs.target_data_location")

        val COMPACTION_STRATEGY = config.getString("compaction.compaction_strategy")
        val ENABLE_NUM_PARTITIONS = config.getBoolean("compaction.enable_num_partitions")
        var NUM_PARTITIONS = config.getInt("compaction.num_partitions")
        val SIZE_RANGES_FOR_COMPACTION: ConfigList = config.getList("compaction.size_ranges_for_compaction")
        val DECODED_SIZE_RANGES_FOR_COMPACTION: Array[AnyRef] = SIZE_RANGES_FOR_COMPACTION.unwrapped().toArray

        val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()

        if (!ENABLE_NUM_PARTITIONS) {

            val hdfs: FileSystem = FileSystem.get(new Configuration())

            def roundUp(d: Double) = math.ceil(d).toInt

            val hadoopPath = new Path(SOURCE_DATA_LOCATION_HDFS)

            val recursive = false
            val ri = hdfs.listFiles(hadoopPath, recursive)
            val it = new Iterator[LocatedFileStatus]() {
                override def hasNext: Boolean = ri.hasNext

                override def next(): LocatedFileStatus = ri.next()
            }

            var partition_size = 256

            // Materialize iterator
            val files = it.toList
            println("No.of files: " + files.size)

            val hdfs_dir_size_in_mb = files.map(_.getLen).sum * 0.000001
            println("Size: " + hdfs_dir_size_in_mb + " MB")

            val hdfs_dir_size_in_gb = hdfs_dir_size_in_mb * 0.001
            println("Size in GB: " + hdfs_dir_size_in_gb)

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

            NUM_PARTITIONS = roundUp(hdfs_dir_size_in_mb / partition_size)
        }
        val df = spark.read.parquet(SOURCE_DATA_LOCATION_HDFS)

        if(COMPACTION_STRATEGY == "rewrite"){
            println("Rewriting Strategy")
            df.repartition(NUM_PARTITIONS).write.mode("overwrite").parquet("/tmp/Spark_Compaction")
        }
        else {
            println("Writing to new Locaton")
            df.repartition(NUM_PARTITIONS).write.parquet(TARGET_DATA_LOCATION_HDFS)
        }

    }

}
