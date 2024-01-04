package com.bbva.datioamproduct.fdevdatio.sesion5.transformations

import org.apache.spark.sql.{DataFrame, Dataset}
import com.bbva.datioamproduct.fdevdatio.sesion5.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.sesion5.fields._

object Join {

  implicit class Join(mapDs: Map[String, Dataset[_]]) {

    def getJoin: DataFrame = {
      val nationalPlayersKeys: Seq[String] = Seq[String](FifaVersion, FifaUpdateDate, NationTeamId, PlayerId)
      val clubPlayersKeys: Seq[String] = Seq[String](FifaVersion, FifaUpdateDate, PlayerId)
      val clubsTeamsKeys: Seq[String] = Seq[String](FifaVersion, FifaUpdateDate, ClubTeamId)
      val nationalTeamsKeys: Seq[String] = Seq[String](NationTeamId, NationalityName)
      val nationalitiesKeys: Seq[String] = Seq[String](NationalityId)

      mapDs(PlayerTag)
        .drop(ClubTeamId.column)
        .join(mapDs(NationalitiesTag), nationalitiesKeys, Left)
        .join(mapDs(NationalPlayersTag), nationalPlayersKeys, Left)
        .join(mapDs(ClubPlayersTag), clubPlayersKeys, Left)
        .join(mapDs(NationalTeamsTag), nationalTeamsKeys, Left)
        .join(mapDs(ClubTeamsTag), clubsTeamsKeys, Left)
    }

  }
}
