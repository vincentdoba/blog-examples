package fr.doba.vincent.aggregator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udaf}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class JsonSchemaAggregatorTest extends AnyFunSuite with should.Matchers {

  val sparkSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test-json-schema-aggregator")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  val json_schema: UserDefinedFunction = udaf(JsonSchemaAggregator)

  test("should merge json object to struct") {
    // Given
    import sparkSession.implicits._
    val input = Seq(
      """{"a": 1, "b": "value1"}""",
      """{"b": "value2", "c": [1, 2, 3]}""",
      """{"a": 3, "d": {"d1" : "value3"}}""",
      """{"d": {"d2" : 4}}"""
    ).toDF("value")

    // When
    val output = input.agg(json_schema(col("value")).alias("schema")).collect().head.getString(0)

    // Then
    output shouldBe "STRUCT<`a`: BIGINT, `b`: STRING, `c`: ARRAY<BIGINT>, `d`: STRUCT<`d1`: STRING, `d2`: BIGINT>>"
  }

  test("should merge json array to array") {
    // Given
    import sparkSession.implicits._
    val input = Seq(
      """[{"a": 1}, {"a": 2}]""",
      """[{"b": "value1"}]""",
      """[{"a": 3, "b": "value2"}]""",
      """[]"""
    ).toDF("value")

    // When
    val output = input.agg(json_schema(col("value")).alias("schema")).collect().head.getString(0)

    // Then
    output shouldBe "ARRAY<STRUCT<`a`: BIGINT, `b`: STRING>>"

  }

}
