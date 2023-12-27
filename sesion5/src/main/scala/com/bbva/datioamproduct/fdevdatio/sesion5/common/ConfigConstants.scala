package com.bbva.datioamproduct.fdevdatio.sesion5.common

object ConfigConstants {

  val Zero: Int = 0

  val RootTag: String = "config"
  val InputTag: String = s"$RootTag.input"

  // PARAMS
  val Params: String = s"$RootTag.params"
  val DevName: String = s"$Params.devName"
  val FifaUpdateDate: String = s"$Params.fifaUpdateDate"
//  val CutoffDate: String = s"$Params.cutoffDate"

  // DATAFRAMES
  val PlayerTag: String = "Players"
  val ClubPlayersTag: String = "ClubPlayers"
  val ClubTeams: String = "ClubTeams"
  val NationalPlayers: String = "NationalPlayers"
  val NationalTeams: String = "NationalTeams"
  val Nationalities: String = "Nationalities"

  val Options: String = "options"
  val OverrideSchema: String = s"$Options.overrideSchema"
  val MergeSchema: String = s"$Options.mergeSchema"
  val PartitionOverwriteMode: String = s"$Options.partitionOverwriteMode"
  val CoalesceNumber: String = s"$Options.coalesce"
  val Delimiter: String = s"$Options.delimiter"
  val Header: String = s"$Options.header"

  val Schema: String = "schema"
  val SchemaPath: String = s"$Schema.path"
  val IncludeMetadataFields: String = s"$Schema.includeMetadataFields"
  val IncludeDeletedFields: String = s"$Schema.includeDeletedFields"


  val Path: String = "path"
  val Table: String = "table"
  val Type: String = "type"

  val PartitionOverwriteModeString: String = s"$Options.partitionOverwriteMode"
  val Partitions: String = "partitions"
  val Mode: String = "mode"

  val DelimiterOption: String = "delimiter"
  val HeaderOption: String = "header"
  val OverrideSchemaOption: String = "overrideSchema"
  val MergeSchemaOption: String = "mergeSchema"

}
