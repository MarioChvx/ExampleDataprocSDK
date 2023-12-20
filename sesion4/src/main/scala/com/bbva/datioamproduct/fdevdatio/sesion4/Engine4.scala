package com.bbva.datioamproduct.fdevdatio.sesion4

import com.bbva.datioamproduct.fdevdatio.sesion4.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
class Engine4 extends SparkProcess with IOUtils{

  override def getProcessId: String = "Engine4"

  override def runProcess(someContext: RuntimeContext): Int = {
    0
  }
}
