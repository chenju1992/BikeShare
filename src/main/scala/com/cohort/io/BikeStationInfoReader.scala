package com.cohort.io

import com.cohort.conf.CohortConf
import com.cohort.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait BikeStationInfoReader extends Logging{

  def readBikeStation(conf: CohortConf, spark: SparkSession): DataFrame = {
    val path = "%s/bike-station-info".format(conf.inputMetaDataPath())

    logInfo("reading from %s".format(path))

    val bikeStationDf = spark.read.json(path)
    Utils.selectColumns(conf, "bike.station.info", bikeStationDf)
  }
}
