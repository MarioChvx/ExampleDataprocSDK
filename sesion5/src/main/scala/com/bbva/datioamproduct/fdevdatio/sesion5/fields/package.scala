package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.extednString
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import scala.collection.Seq
import scala.language.implicitConversions

package object fields {

  implicit def fieldToColumn(field: Field): Column = field.column
  implicit def fieldToString(field: Field): String = field.name

  trait Field {
    val name: String = name
    lazy val column: Column = col(name)
    def apply():Column = column
  }

  // ONLY COMMON FIELDS
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
  }

  case object PlayerPositions extends Field {
    override val name: String = "player_positions"

    override def apply(): Column =
      split(regexp_replace(column, " ", ""), Comma) as name
  }

  case object ExplodePlayerPositions extends Field {
    override val name: String = "explode_player_positions"

    override def apply(): Column = explode(PlayerPositions.column) as name
  }

  case object CountByPlayerPositions extends Field {
    override val name: String = "count_by_position"

    override def apply(): Column = count(ExplodePlayerPositions.column) as name
  }

  case object FifaVersion extends Field {
    override val name: String = "fifa_version"
  }

  case object FifaUpdateDate extends Field {
    override val name: String = "fifa_update_date"
  }

  case object LeagueName extends Field {
    override val name: String = "league_name"
  }

  case object ClubName extends Field {
    override val name: String = "club_name"
  }

  case object NationalityName extends Field {
    override val name: String = "nationality_name"
  }

  case object NationalityId extends Field {
    override val name: String = "nationality_id"
  }

  case object Age extends Field {
    override val name: String = "age"
  }

  case object CatAge extends Field {
    override val name: String = "cat_age_overall"

    override def apply(): Column =
      when(Age.column <= 20 || Overall.column > 80, A)
        .when(Age.column <= 23 || Overall.column > 70, B)
        .when(Age.column <= 30,  C)
        .otherwise(D)
        .as(name)
  }

  case object ZScore extends Field {
    override val name: String = "z_score"

    def apply(by: Field*): Column = {
      val columns: Seq[Column] = by.map(_.column)
      val window: WindowSpec = Window.partitionBy(columns:_*)//.orderBy(Overall.column)
      ((Overall.column - mean(Overall.column).over(window)) / stddev(Overall.column).over(window)) as name
    }
  }

  case object LongName extends Field {
    override val name: String = "long_name"
  }

  case object LeagueId extends Field {
    override val name: String = "league_id"
  }

  case object CollectedTeamName extends Field {
    override val name: String = "unique_team_name"

    override def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(ClubTeamId.column)
//      collect_set(ClubName.column).over(window) as name
      first(ClubName.column).over(window.orderBy(length(ClubName).desc))
    }
  }

  case object UniqueTeamName extends Field {
    override val name: String = "unique_team_name"

    override def apply(): Column = {
      val maxLength = udf((x: Seq[String]) => x.reduce((a,b) => if (a.length > b.length) a else b))
      maxLength(CollectedTeamName) as name
    }
  }

  case object SumOverall extends Field {
    override val name: String = "sum_overall"

    override def apply(): Column = {
      val window: WindowSpec = Window.partitionBy(Seq[Column](ClubName, CatAge):_*).orderBy(Overall.column)
      sum(Overall.column) over window as name
    }
  }
}
