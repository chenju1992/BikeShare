package com.cohort.process

import com.cohort.conf.CohortConf
import com.cohort.io._
import com.cohort.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProcess extends Logging with UserReader with BikeShareTripReader {

  val spark = SparkSession
    .builder()
    .appName("Unique-users")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val conf = new CohortConf(args)

    val inputPath = Utils.pathGenerator(conf.inputBikeSharePath(), conf.datePrefix(), conf.processDate())

    uniqueUser(conf.uniqueUserPath(), inputPath, conf)
  }

  def uniqueUser(uniqueUsersPath: String, inputBikeSharePath: String, conf: CohortConf): Unit = {

    val inputUniqueUsersDf = readUserInfo(conf, spark)
    val inputBikeShareDf = readBikeShareTrip(conf, spark)

    val users = Utils.selectColumns(conf, "bike.unique.user", inputBikeShareDf)
      .withColumn("first_timestamp", col("start_timestamp"))
      .drop(col("start_timestamp"))

    val uniqueUserDf = inputUniqueUsersDf.union(users)
      .groupBy("user_id")
      .agg(min("first_timestamp").as("first_timestamp"))

    uniqueUserDf.distinct().coalesce(1).write.mode(SaveMode.Overwrite).json(uniqueUsersPath)
  }

}
