package io.github.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseFilter
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class AssignPartition extends BaseFilter {

  var conf: Config = ConfigFactory.empty()

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.conf = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = {
    this.conf
  }

  override def checkConfig(): (Boolean, String) = {
    conf.hasPath("target_field") && (conf.hasPath("value") || conf.hasPath("source_field")) match {
      case true => (true, "")
      case false => (false, "please specify [target_field], [value or source_field]")
    }
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    conf.hasPath("source_field") match {
      case true => df.withColumn(conf.getString("target_field"), df(conf.getString("source_field")))
      case false => df.withColumn(conf.getString("target_field"), lit(conf.getString("value")))
    }
  }
}