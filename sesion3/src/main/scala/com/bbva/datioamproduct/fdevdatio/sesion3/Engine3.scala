package com.bbva.datioamproduct.fdevdatio.sesion3

import com.bbva.datioamproduct.fdevdatio.sesion3.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.execution.command.ClearCacheCommand.printSchema

class Engine3 extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def getProcessId: String = "Engine3"

  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val clubPlayersDs = read(config.getConfig("sesion3.input.ClubPlayers"))
    val clubTeamsDs = read(config.getConfig("sesion3.input.ClubTeams"))
    val nationalPlayersDs = read(config.getConfig("sesion3.input.NationalPlayers"))
    val nationalTeamsDs = read(config.getConfig("sesion3.input.NationalTeams"))
    val nationalitiesDs = read(config.getConfig("sesion3.input.Nationalities"))
    val playersDs = read(config.getConfig("sesion3.input.Players"))
    //    logger.info(s"schema.toDDl: ${clubTeamsDs.schema.toDDL}")
    clubPlayersDs.printSchema()
    clubTeamsDs.printSchema()
    nationalPlayersDs.printSchema()
    nationalTeamsDs.printSchema()
    nationalitiesDs.printSchema()
    playersDs.printSchema()
    0
  }

}
