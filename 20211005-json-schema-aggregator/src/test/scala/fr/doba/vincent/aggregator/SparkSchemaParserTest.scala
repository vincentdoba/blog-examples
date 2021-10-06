package fr.doba.vincent.aggregator

import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class SparkSchemaParserTest extends AnyFunSuite with should.Matchers {

  test("should parse simple type json") {
    // Given
    val json = """{"a": 1, "b": 2.3, "c": "value"}"""

    // When
    val dataType = SparkSchemaParser.fromJson(json)

    // Then
    dataType shouldBe StructType(Seq(StructField("a", LongType), StructField("b", DoubleType), StructField("c", StringType)))
  }

  test("should parse empty array") {
    // Given
    val json = "[]"

    // When
    val dataType = SparkSchemaParser.fromJson(json)

    // Then
    dataType shouldBe ArrayType(StringType)
  }



}
