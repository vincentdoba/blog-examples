package fr.doba.vincent.aggregator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udaf}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StringType, StructField, StructType}
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class JsonSchemaAggregatorTest extends AnyFunSuite with should.Matchers with OptionValues {

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
    val expectedDatatype = DataType.fromDDL("STRUCT<`a`: BIGINT, `b`: STRING, `c`: ARRAY<BIGINT>, `d`: STRUCT<`d1`: STRING, `d2`: BIGINT>>").asInstanceOf[StructType]
    val actualDatatype = DataType.fromDDL(output).asInstanceOf[StructType]

    actualDatatype.fields should have size 4
    actualDatatype.fields.find(_.name == "a").value shouldBe StructField("a", LongType)
    actualDatatype.fields.find(_.name == "b").value shouldBe StructField("b", StringType)
    actualDatatype.fields.find(_.name == "c").value shouldBe StructField("c", ArrayType(LongType))
    actualDatatype.fields.find(_.name == "d").value.dataType.asInstanceOf[StructType].fields.sortBy(_.name) shouldBe
      expectedDatatype.fields.find(_.name == "d").value.dataType.asInstanceOf[StructType].fields.sortBy(_.name)

  }

  test("should merge struct json array to array") {
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

  test("should merge simple type json array to array") {
    // Given
    import sparkSession.implicits._
    val input = Seq(
      """[1, 2]""",
      """[3]""",
      """[]""",
      """[4, 5]"""
    ).toDF("value")

    // When
    val output = input.agg(json_schema(col("value")).alias("schema")).collect().head.getString(0)

    // Then
    output shouldBe "ARRAY<BIGINT>"
  }

  test("should generate schema with nested array") {
    // Given
    import sparkSession.implicits._
    val input = Seq(
      """{"a": {"b" : []}}""",
      """{"a": {"b" : [{"c": 1}]}}"""
    ).toDF("value")

    // When
    val output = input.agg(json_schema(col("value")).alias("schema")).collect().head.getString(0)

    // Then
    println(output)
  }

}
