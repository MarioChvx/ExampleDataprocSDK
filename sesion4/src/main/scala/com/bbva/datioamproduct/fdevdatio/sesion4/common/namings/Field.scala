package com.bbva.datioamproduct.fdevdatio.sesion4.common.namings

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait Field {
  val name: String
  lazy val column: Column = col(name)
}
