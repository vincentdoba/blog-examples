package fr.doba.vincent

import fr.doba.vincent.aggregator.JsonSchemaAggregator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, udaf}
import org.apache.spark.sql.types.DataType

object JsonSchemaAggregatorApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("json-schema-aggregator")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    val input = Seq(
      """{"a": 1, "b": "value1"}""",
      """{"b": "value2", "c": [1, 2, 3]}""",
      """{"a": 3, "d": {"d1" : "value3"}}""",
      """{"d": {"d2" : 4}}"""
    ).toDF("value")

    input.show(false)

    val json_schema = udaf(JsonSchemaAggregator)

    val output = input.agg(json_schema(col("value")).alias("schema"))

    output.show(false)

    input.select(from_json(
      col("value"),
      DataType.fromDDL(output.collect().head.getString(0))
    )).printSchema()
  }

}
