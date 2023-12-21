package com.bbva.datioamproduct.fdevdatio.sesion4

import com.bbva.datioamproduct.fdevdatio.sesion4.common.ConfigConstants.InputTag
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {
  implicit class configExtension(config: Config) extends IOUtils {
    def readParquets: Map[String, Dataset[Row]] = {
      config.getObject(InputTag).keySet()
        .map((key: String) => {
            (key, read(config.getConfig(s"$InputTag.$key")))
        }).toMap
    }
  }
}
