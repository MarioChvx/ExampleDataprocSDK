package com.bbva.datioamproduct.fdevdatio.sesion5

import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.bbva.datioamproduct.fdevdatio.sesion5.fields.PlayersFields
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import scala.language.implicitConversions

package object fields {

  implicit def fieldToColumn(field: Field): Column = field.column
//  implicit def fieldToString(field: Field): String = field.name

  trait Field {
    val name: String = name
    lazy val column: Column = col(name)
    def apply():Column = column
    def other(): Column = column
    def asColumn(): Column = col(name)
  }

  case object PlayerId extends Field {
    override val name: String = "player_id"

  }

  case object ClubTeamId extends Field {
    override val name: String = "club_team_id"
  }

  case object NationTeamId extends Field {
    override val name: String = "nation_team_id"
  }

  case object HeightCm extends Field {
    override val name: String = "height_cm"
  }

  case object Overall extends Field {
    override val name: String = "overall"
  }

  case object CatHeight extends Field {
    override val name: String = "cat_height"

    override def apply(): Column = {
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


    override def apply(): Column = lit("nuevo valor").as(name)

    override def other(): Column = lit("nuevo valor").as(name)
  }

}
