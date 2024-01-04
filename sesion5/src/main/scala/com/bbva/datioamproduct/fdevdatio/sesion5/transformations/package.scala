package com.bbva.datioamproduct.fdevdatio.sesion5

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._
import com.bbva.datioamproduct.fdevdatio.sesion5.fields.fieldToColumn
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import java.util.Date

package object transformations {

  implicit class extendField(f: Field) {
    def getFieldName:String = f.expr.asInstanceOf[NamedExpression].name
  }

  implicit class extednString(s: String) {
//    def isLonger(str: String): Boolean = str match {
//      case (s.length > str.length) => true
//      case _ => false
//    }
  }

  implicit class Transformations(ds: Dataset[Row]) {

    private val UpdateDateCol: String = "fifa_update_date"
    def getMaxUpdateDate: Date = {
//      ds
//        .select(f.col(UpdateDateCol))
//        .rdd
//        .map(row => row.getAs[Date](UpdateDateCol))
//        .reduce(
//          (a, b) => if (a.after(b)) a else b
//        )
      ds
        .select(
          max(col(UpdateDateCol)).as("sum")
        )
        .first
        .getAs[Date]("sum")
      // Iintente hacerlo con map y reduce pero me salia un error, algo que la tarea no era serializable
    }

    def filterPlayersByDate(date: String): Dataset[Row] = {
      ds.filter(date_format(col(UpdateDateCol), "yyyy-MM-dd") === date)
    }

    def addColumn(columns: Column*): Dataset[Row] = ds
      .select(
        ds.columns.map(col) ++ columns :_*
      )

    def replaceColumn(field: Field): DataFrame = {
      val columnName: String = field.getFieldName
      val columnValues: Column = field.apply()
      val columns: Array[Column] = ds.columns.map {
        case name: String if name == columnName => columnValues
        case _@name => col(name)
      }
      ds.select(columns: _*)
    }

    def notIn(ds2: DataFrame, by: Field*): DataFrame= {
      val keyNames = by.map(_.getFieldName)
      ds.join(ds2, Seq(keyNames :_*), "left_anti")
    }

    def in(ds2: DataFrame, by: Field*): DataFrame = {
      val keyNames = by.map(_.getFieldName)
      ds.join(ds2, Seq(keyNames :_*), "left_semi")
    }

    def explodePlayerPositions: DataFrame =
      ds
        .replaceColumn(PlayerPositions)
        .addColumn(ExplodePlayerPositions())
    def countPlayerPositions: DataFrame =
      ds
        .explodePlayerPositions
        .groupBy(ExplodePlayerPositions.column)
        .agg(CountByPlayerPositions())

    def overallMean(league: String, position: String): DataFrame =
      ds
        .filter(LeagueName === league)
        .explodePlayerPositions
        .filter(ExplodePlayerPositions === position)
        .agg(mean(Overall.column))
        .as(s"Overall from $league, at position $position")

    def teamWithBest(league: String, position: String): DataFrame =
      ds
        .filter(LeagueName === league)
        .explodePlayerPositions
        .filter(ExplodePlayerPositions === position)
        .groupBy(ClubName.column)
        .agg(mean(Overall.column).as(Overall.name))
        .sort(Overall.column.desc)

    def colIntersect(right: Dataset[_]): Seq[String] = {
      val leftColumns = ds.columns.seq
      val rightColumns = right.columns.seq
      leftColumns.intersect(rightColumns)
    }
    def joinWithoutAmbiguities(right:Dataset[_], joinExpr: Column, joinType: String): DataFrame =
      ds
        .join(
          right.select(
            ds.colIntersect(right)
              .diff(joinExpr.toString)
              .map(col):_*
          ),
          joinExpr,
          joinType
        )

    def getUniqueClubTeams: DataFrame = ds
      .addColumn(CollectedTeamName())
//      .replaceColumn(UniqueTeamName)
  }
}
