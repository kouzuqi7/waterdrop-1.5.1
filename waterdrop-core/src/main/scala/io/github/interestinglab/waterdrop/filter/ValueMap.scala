package io.github.interestinglab.waterdrop.filter

import net.minidev.json.{JSONObject}
import net.minidev.json.parser.JSONParser
import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

import scala.collection.JavaConversions._
import scala.collection.mutable

class ValueMap extends BaseFilter {
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

  override def checkConfig(): (Boolean, String) = (true, "")

  override def getUdfList(): List[(String, UserDefinedFunction)] = {
    def valueMapUdf(str: String, defaultValue: String, valueMap: String): String = {
      var map = json2Map(valueMap).toMap
      var strNew = defaultValue
      for ((key, value) <- map) {
        if (key == str) {
          strNew = value
        }
      }
      strNew
    }
    val func = udf(valueMapUdf _)
    List(("udf_valueMapUdf", func))
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val udf_valueMapUdf = getUdfList().get(0)._2
    df.withColumn(conf.getString("target_field"),
      udf_valueMapUdf(
        df(conf.getString("source_field")),
        lit(conf.getString("defaultValue")),
        lit(conf.getString("valueMap"))
      )
    )
  }

  /**
   * 将json转化为Map
   * @param json 输入json字符串
   * @return
   * */
  def json2Map(json : String) : mutable.HashMap[String,String] = {
    val map : mutable.HashMap[String,String]= mutable.HashMap()
    val jsonParser =new JSONParser()
    //将string转化为jsonObject
    val jsonObj: JSONObject = jsonParser.parse(json).asInstanceOf[JSONObject]
    //获取所有键
    val jsonKey = jsonObj.keySet()
    val iter = jsonKey.iterator()

    while (iter.hasNext){
      val field = iter.next()
      val value = jsonObj.get(field).toString
      if(value.startsWith("{")&&value.endsWith("}")){
        val value = jsonObj.get(field).asInstanceOf[String]
        map.put(field,value)
      }else{
        map.put(field,value)
      }
    }
    map
  }
}