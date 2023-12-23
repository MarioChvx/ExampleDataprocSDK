package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants.{DevName, PlayerTag}
import com.bbva.datioamproduct.fdevdatio.sesion5.common.{Player, configExtension}
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date

class Engine5 extends SparkProcess with IOUtils{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def getProcessId: String = "Engine5"
  override def runProcess(someContext: RuntimeContext): Int = {
    val config: Config = someContext.getConfig
    val fifaDs = config.readParquets
    val playersRDD: RDD[Row] = fifaDs(PlayerTag).rdd

    // Count by nationality
    playersRDD
      .filter(
        row =>
          row.getAs[Date]("fifa_update_date").toString == "2022-07-18"
          && row.getAs[Int]("nationality_id") != null
      )
      .map(
        row => (row.getAs[Int]("nationality_id"), 1)
      )
      .reduceByKey(_ + _)
      .filter(_._1 == 83)
//      .foreach(println)


    val lessCommonTrait = playersRDD
//    playersRDD
      .filter(
        row =>
          row.getAs[Date]("fifa_update_date").toString == "2022-01-18"
            && row.getAs[String]("player_traits") != null
      )
      .map(
        _.getAs[String]("player_traits")
          .replaceAll(", ", ",")
      )
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .reduce(
        (a, b) => if (a._2 < b._2) a else b
      )

    println(s"Less common trait $lessCommonTrait")

    val bestSkillBallControl = playersRDD
      .filter(_.getAs[Date]("fifa_update_date").toString == "2022-07-18")
      .map(
        row => Player(
          row.getAs[String]("short_name"),
          row.getAs[Int]("overall"),
          row.getAs[Int]("skill_ball_control")
        )
      )
      .reduce(
        (a, b) => if (a.skillBallControl > b.skillBallControl) a else b
      )

    println(s"Best skill ball control player $bestSkillBallControl")

    0
  }
}
