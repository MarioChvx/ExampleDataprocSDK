package com.bbva.datioamproduct.fdevdatio.sesion5

import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants.{ClubTeamsTag, Zero}
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.Join.Join
import com.bbva.datioamproduct.fdevdatio.sesion5.utils.{IOUtils, configExtension}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, functions}
import com.bbva.datioamproduct.fdevdatio.sesion5.transformations.Transformations
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._
import org.apache.spark.sql.functions.when

class Engine10 extends SparkProcess with IOUtils{

  override def getProcessId: String = "Engine10"

  override def runProcess(someContext: RuntimeContext): Int = {

    val config: Config = someContext.getConfig
    val fifaDs: Map[String, DataFrame] = config.readParquets
    val fullJoin: DataFrame = fifaDs.getJoin

//    fifaDs.foreach(x => println(x._1 + " -> " + x._2.columns.seq.toString))

    val newDs = fullJoin
//      .addColumn(CatAge())
//      .addColumn(ZScore(NationalityName, CatAge))
//      .select(Seq[Column](LongName, NationalityName, LeagueName, ZScore):_*)
//      .sort(ZScore.column.desc)
//
//    // Mexico
//    newDs
//      .filter(NationalityName === "Mexico")
//      .show(10)
//    // Liga MX
//    newDs
//      .filter(LeagueName === "Liga MX")
//      .show(10)

    fifaDs(ClubTeamsTag)
      .select(Seq[Column](ClubTeamId, ClubName):_*)
      .getUniqueClubTeams
      .filter(!(UniqueTeamName.column === ClubName.column))
      .show()

//    newDs
//      .select(Seq[Column](ShortName, ClubName, Overall, Age):_*)
//      .addColumn(CatAge())
//      .addColumn(SumOverall())
//      .filter(ClubName.column === "Necaxa")
//      .show(100, false)

    Zero
  }

}
