package com.cohort.io

import com.cohort.conf.CohortConf
import com.cohort.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

trait BikeShareTripReader extends Logging{

  def readBikeShareTrip(conf: CohortConf, spark: SparkSession): DataFrame = {
    val path = Utils.pathGenerator(conf.inputBikeSharePath(), conf.datePrefix(), conf.processDate())

    logInfo("reading from %s".format(path))

    val bikeShareDf: DataFrame = try {
      Some(spark.read.json(path)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("duration_sec", lit(null: DoubleType))
        .withColumn("start_timestamp", lit(null: StringType))
    }
    Utils.selectColumns(conf, "bike.share.trip", bikeShareDf)
  }

  def readDayAgoBikeShareTrip(conf: CohortConf, spark: SparkSession): DataFrame = {
    val path = dayAgoReadDataOutPath(conf)

    logInfo("reading from %s".format(path))

    val bikeShareDf: DataFrame = try {
      Some(spark.read.json(path)).get
    } catch {
      case e: Exception => spark.emptyDataFrame
        .withColumn("user_id", lit(null: StringType))
        .withColumn("subscriber_type", lit(null: StringType))
        .withColumn("start_station_id", lit(null: StringType))
        .withColumn("end_station_id", lit(null: StringType))
        .withColumn("zip_code", lit(null: StringType))
        .withColumn("avg_duration_sec", lit(null: DoubleType))
    }
    bikeShareDf
  }

  def dayAgoReadDataOutPath(conf: CohortConf): String = {
    val dateString = dayAgoDateString(conf)

    val path : String = conf.dayAgo() match {
      case 1 => Utils.pathGenerator(conf.outputDataPath(), conf.datePrefix(), dateString)
      case 3 => Utils.pathGenerator(conf.outputDataPath()+"/1", conf.datePrefix(), dateString)
      case 7 => Utils.pathGenerator(conf.outputDataPath()+"/3", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }

  def dayAgoWriteDataOutPath(conf: CohortConf): String = {
    val dateString = dayAgoDateString(conf)

    val path : String = conf.dayAgo() match {
      case 1 => Utils.pathGenerator(conf.outputDataPath()+"/1", conf.datePrefix(), dateString)
      case 3 => Utils.pathGenerator(conf.outputDataPath()+"/3", conf.datePrefix(), dateString)
      case 7 => Utils.pathGenerator(conf.outputDataPath()+"/7", conf.datePrefix(), dateString)
      case _ => throw new Exception("input date is invalid")
    }
    path
  }

  def dayAgoDateString(conf: CohortConf): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val processDate: DateTime         = DateTime.parse(conf.processDate(), dateFormat)
    dateFormat.print(processDate.minusDays(conf.dayAgo()))
  }

}
