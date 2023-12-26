package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants.InputTag
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.IOUtils
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import java.util.Date
import scala.collection.convert.ImplicitConversions.`set asScala`

package object common {
  case class Player(name: String, overall: Int,  skillBallControl: Int)

}
