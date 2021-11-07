package fr.doba.vincent.aggregator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class SparkSchemaParserTest extends AnyFunSuite with should.Matchers {

  test("should parse complexe types") {
    // Given
    val json = """{"a": 1, "b": 2.3, "c": "value", "d": {"d1": 1, "d2": 2}, "e": [1, 2, 3], "f": [{"a": true}, {"a": false}]}"""

    // When
    val dataType = SparkSchemaParser.fromJson(json)

    // Then
    dataType shouldBe Map(
      "f[].a" -> "BOOLEAN",
      "a" -> "BIGINT",
      "d.d2" -> "BIGINT",
      "b" -> "DOUBLE",
      "e[]" -> "BIGINT",
      "c" -> "STRING",
      "d.d1" -> "BIGINT"
    )

  }


  test("should parse simple type json") {
    // Given
    val json = """{"a": 1, "b": 2.3, "c": "value", "d": true}"""

    // When
    val dataType = SparkSchemaParser.fromJson(json)

    // Then
    dataType shouldBe Map("a" -> "BIGINT", "b" -> "DOUBLE", "c" -> "STRING", "d" -> "BOOLEAN")
  }

  test("should parse empty array") {
    // Given
    val json = "[]"

    // When
    val dataType = SparkSchemaParser.fromJson(json)

    // Then
    dataType shouldBe Map("[]" -> "STRING")
  }

}
