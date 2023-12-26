package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants.{FifaUpdateDate, PlayerTag}
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.Transformations
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.{IOUtils, configExtension}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, Dataset, Row, functions => f}
import org.slf4j.{Logger, LoggerFactory}
import java.util.Date

class Engine6 extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "Engine6"

  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val fifaDs = config.readParquets
    val playersDs: Dataset[Row] = fifaDs(PlayerTag)
//    val hola = f.col("x") >= f.col("y")
//    def =!= (other: Any): Column = ???
    println(s"Max update date ${playersDs.getMaxUpdateDate}")
    val targetDate: String = config.getString(FifaUpdateDate)
    playersDs.filterPlayersByDate(targetDate).show
    0
  }

}
