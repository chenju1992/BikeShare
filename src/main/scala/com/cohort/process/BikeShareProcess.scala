package com.cohort.process

import com.cohort.conf.CohortConf
import com.cohort.io.BikeShareTripReader
import com.cohort.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BikeShareProcess extends Logging with BikeShareTripReader {

  val fields = List("user_id",
    "subscriber_type",
    "start_station_id",
    "end_station_id",
    "zip_code")

  val avgDurationSec = "avg_duration_sec"

  def main(args: Array[String]): Unit = {
    val conf                  = new CohortConf(args)
    val spark = SparkSession
      .builder()
      .appName("Bike-share")
      .getOrCreate()

    val outputPath = Utils.pathGenerator(conf.outputDataPath(), conf.datePrefix(), conf.processDate())

    bikeShareAgg(spark, conf, outputPath)
  }

  def bikeShareAgg(spark: SparkSession, conf: CohortConf, outputPath: String): Unit = {

    val bikeShareDf = readBikeShareTrip(conf, spark)

    val bikeShareAggDf = bikeShareDf
      .groupBy(fields.map(col):_*)
      .agg(avg(col("duration_sec")).as(avgDurationSec))

    bikeShareAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }

}
