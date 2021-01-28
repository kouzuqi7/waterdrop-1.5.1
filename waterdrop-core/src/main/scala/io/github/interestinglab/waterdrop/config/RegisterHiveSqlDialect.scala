package io.github.interestinglab.waterdrop.config

import org.apache.spark.sql.jdbc.JdbcDialects

object RegisterHiveSqlDialect {
  def register(): Unit = {
    JdbcDialects.registerDialect(HiveSqlDialect)
  }
}
