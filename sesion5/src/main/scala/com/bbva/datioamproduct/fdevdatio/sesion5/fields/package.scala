package com.bbva.datioamproduct.fdevdatio.sesion5

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._

import scala.language.implicitConversions

package object fields {

  implicit def fieldToColumn(field: Field): Column = field.column

  trait Field {
    val name: String = name
    lazy val column: Column = col(name)

  }

  case object HeightCm extends Field {
    override val name: String = "height_cm"
  }

  case object Overall extends Field {
    override val name: String = "overall"
  }
  case object CatHeight extends Field {
    override val name: String = "cat_height"

    def apply(): Column = {
      when(HeightCm > 200, "A")
        .when(HeightCm >= 185, "B")
        .when(HeightCm > 175, "C")
        .when(HeightCm >= 165, "D")
        .otherwise("E")
        .alias(name)
    }
  }

  case object CutoffDate extends Field {
    override val name: String = "cutoff_date"

    def filter(date: String): Column = date_format(column, "yyyy-MM-dd") === date
  }

  case object ShortName extends Field {
    override val name: String = "short_name"

    def apply(): Column = lit("nuevo valor") alias name
  }

}
