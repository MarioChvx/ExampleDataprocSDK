package com.bbva.datioamproduct.fdevdatio.sesion5

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions => f}
import com.bbva.datioamproduct.fdevdatio.sesion5.fields.Field
import com.bbva.datioamproduct.fdevdatio.sesion5.fields.fieldToColumn
import org.apache.spark.sql.catalyst.expressions.NamedExpression

import java.util.Date

package object transformations {

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
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name
      val columnColumn: Column = field.expr.asInstanceOf[NamedExpression].apply()
      val columns: Array[Column] = ds.columns.map {
//        case name: String if name == columnName => field.apply()
//        case name: String if name == columnName => lit("nuevo valor") as columnName
        case name: String if name == columnName => columnColumn
        case _@name => f.col(name)
      }
      ds.select(columns: _*)
    }

  }

}
