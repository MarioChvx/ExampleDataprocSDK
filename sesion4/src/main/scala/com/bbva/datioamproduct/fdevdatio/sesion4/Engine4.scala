package com.bbva.datioamproduct.fdevdatio.sesion4

import com.bbva.datioamproduct.fdevdatio.sesion4.common.ConfigConstants.DevName
import com.bbva.datioamproduct.fdevdatio.sesion4.utils.{IOUtils, configExtension}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

class Engine4 extends SparkProcess with IOUtils{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def getProcessId: String = "Engine4"

  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val fifaDs = config.readParquets
    logger.info(s"${config.getString(DevName)}")
    fifaDs("ClubTeams").show
    0
  }
}
