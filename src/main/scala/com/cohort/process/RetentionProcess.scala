package com.cohort.process

import com.cohort.conf.CohortConf
import com.cohort.io._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RetentionProcess extends Logging with UserReader with BikeShareTripReader {

  def main(args: Array[String]): Unit = {
    val conf                  = new CohortConf(args)
    val spark = SparkSession
      .builder()
      .appName("Bike-share")
      .getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    retentionPrep(spark, conf)
  }

  def retentionPrep(spark: SparkSession, conf: CohortConf): Unit = {
    val bikeShareDf = readBikeShareTrip(conf, spark)
    val userDf = readUserInfo(conf, spark)
    val dayAgoBikeShareDf = readDayAgoBikeShareTrip(conf, spark)

    val joinedBikeShareDf = bikeShareDf
      .join(userDf, bikeShareDf.col("user_id") === userDf.col("user_id") , "left")
      .drop(bikeShareDf.col("user_id"))

    import spark.implicits._

    val bikeUserAgeDays = joinedBikeShareDf.withColumn("user_age_days",
      floor((unix_timestamp($"start_timestamp", "yyyy-MM-dd'T'HH:mm:ss")
        - unix_timestamp($"first_timestamp", "yyyy-MM-dd'T'HH:mm:ss")) / 86400.0) )

    val bikeFilteredDf : DataFrame = conf.dayAgo() match {
      case 1 => bikeUserAgeDays.filter(col("user_age_days") === 1)
      case 3 => bikeUserAgeDays.filter(col("user_age_days") === 3)
      case 7 => bikeUserAgeDays.filter(col("user_age_days") === 7)
      case _ => throw new Exception("input date is invalid")
    }

    val bikeFilteredAgoDf = bikeFilteredDf.select("user_id", "user_age_days").distinct()

    val aggPrepDf = dayAgoBikeShareDf.join(bikeFilteredAgoDf,
      dayAgoBikeShareDf.col("user_id") === bikeFilteredAgoDf.col("user_id"), "left")
      .drop(bikeFilteredAgoDf.col("user_id"))

    val groupbyFields = BikeShareProcess.fields :+ BikeShareProcess.avgDurationSec

    val bikeUserAggDf = aggPrepDf.groupBy(groupbyFields.map(col):_*)
      .agg(
        max(when(col("user_age_days") === 1, 1).otherwise(0)).alias("retention_1"),
        max(when(col("user_age_days") === 3, 1).otherwise(0)).alias("retention_3"),
        max(when(col("user_age_days") === 7, 1).otherwise(0)).alias("retention_7")
      )

    val outputPath = dayAgoWriteDataOutPath(conf)
    bikeUserAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }

}
