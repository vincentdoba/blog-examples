package fr.doba.vincent.aggregator

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class SparkSchemaMergerTest extends AnyFunSuite with should.Matchers {

  test("should default to other element type when array element is string") {
    // Given
    val schema1 = ArrayType(StringType)
    val schema2 = ArrayType(IntegerType)

    // When
    val dataType1 = SparkSchemaMerger.mergeArrayTypes(schema1, schema2)
    val dataType2 = SparkSchemaMerger.mergeArrayTypes(schema2, schema1)

    // Then
    dataType1 shouldBe ArrayType(IntegerType)
    dataType2 shouldBe ArrayType(IntegerType)
  }





}
