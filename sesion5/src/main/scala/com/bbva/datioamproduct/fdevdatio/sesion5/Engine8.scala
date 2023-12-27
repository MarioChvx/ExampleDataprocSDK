package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.Transformations
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.{IOUtils, Params, configExtension}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class Engine8 extends SparkProcess with IOUtils{

  override def getProcessId: String = "Engine8"

  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val fifaDs = config.readParquets
    val params: Params = config.getParams
    val playersDs: DataFrame = fifaDs(PlayerTag)//.filterPlayersByDate(params.fifaUpdateDate)
    val nationalPlayersDs: DataFrame = fifaDs(NationalPlayers)//.filterPlayersByDate(params.fifaUpdateDate)
    val clubPlayers: DataFrame = fifaDs(ClubPlayersTag)

    val res =
    playersDs
      .join(nationalPlayersDs, Seq(PlayerId.name), "left_anti")
      .count

    println(s"aqui: $res")

    println(playersDs.columns.toSeq.intersect(clubPlayers.columns.toSeq))
    println(playersDs.columns.toSeq.intersect(nationalPlayersDs.columns.toSeq))

//    playersDs.notIn(clubPlayers, PlayerId).show()
    val res2: Long =
      playersDs
        .filterPlayersByDate(params.fifaUpdateDate)
        .filter(col("nationality_id") === 83)
        .in(nationalPlayersDs.filterPlayersByDate(params.fifaUpdateDate), PlayerId)
        .notIn(clubPlayers.filterPlayersByDate(params.fifaUpdateDate), PlayerId)
        .count

    println(s"Update date: ${params.fifaUpdateDate}")
    println(s"jugadores con fifa_update_date=2022-09-01 que juegan en una selección nacional pero no cuentan con información del club en el que juegan $res2")

    Zero
  }
}
