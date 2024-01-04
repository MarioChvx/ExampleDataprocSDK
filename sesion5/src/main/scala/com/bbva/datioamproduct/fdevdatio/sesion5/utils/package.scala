package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants._
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {

  case class Params(devNem: String, fifaUpdateDate: String)

  implicit class configExtension(config: Config) extends IOUtils {
    def readParquets: Map[String, Dataset[Row]] = {
      config.getObject(InputTag).keySet()
        .map((key: String) => {
          (key, read(config.getConfig(s"$InputTag.$key")))
        }).toMap
    }

//    def getParams: Map[String, String] =
//      config.getObject(Params).keySet()
//        .map((key: String) => {
//          (key, config.getString(s"$Params.$key"))
//        }).toMap

    def getParams: Params = Params(
      config.getString(DevName),
      config.getString(PartitionDate)
    )

  }
}
