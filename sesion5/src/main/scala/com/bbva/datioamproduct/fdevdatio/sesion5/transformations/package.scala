package com.bbva.datioamproduct.fdevdatio.sesion5

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => f}
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._
import com.bbva.datioamproduct.fdevdatio.sesion5.fields.fieldToColumn
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.Date

package object transformations {

  implicit class extendField(f: Field) {
    def getFieldName:String = f.expr.asInstanceOf[NamedExpression].name
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
          f.max(f.col(UpdateDateCol)).as("sum")
        )
        .first
        .getAs[Date]("sum")
      // Iintente hacerlo con map y reduce pero me salia un error, algo que la tarea no era serializable
    }

    def filterPlayersByDate(date: String): Dataset[Row] = {
      ds.filter(f.date_format(f.col(UpdateDateCol), "yyyy-MM-dd") === date)
    }

    def addColumn(columns: Column*): Dataset[Row] = ds
      .select(
        ds.columns.map(col) ++ columns :_*
      )

    def replaceColumn(field: Field): DataFrame = {
      val columnName: String = field.getFieldName
//      val columnName: String = field.expr.asInstanceOf[NamedExpression].name
      val columnColumn: Column = field()
      val columns: Array[Column] = ds.columns.map {
        case name: String if name == columnName => columnColumn
        case _@name => f.col(name)
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
  }

}
