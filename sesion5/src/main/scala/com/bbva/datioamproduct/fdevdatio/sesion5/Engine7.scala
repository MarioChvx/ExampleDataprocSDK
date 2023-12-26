package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.Transformations
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.{IOUtils, Params, configExtension}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

class Engine7 extends SparkProcess with IOUtils{

  override def getProcessId: String = "Engine7"

  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val fifaDs = config.readParquets
    val playersDs: Dataset[Row] = fifaDs(PlayerTag)
    val date: String = config.getString(FifaUpdateDate)
    val paramsObj: Params = config.getParams


//    playersDs.select(col("short_name"), col("height_cm"), CatHeight()).show

//    playersDs
//      .filterPlayersByDate(paramsObj.cutoffDate)
//      .addColumn(CatHeight())
//      .groupBy(CatHeight)
//      .agg(avg(Overall))
//      .show

      playersDs
        .replaceColumn(ShortName)
        .show()
    0
  }
}
