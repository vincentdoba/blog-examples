package fr.doba.vincent.aggregator

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{Encoder, Encoders}

object JsonSchemaAggregator extends Aggregator[String, String, String] {

  override def zero: String = null

  override def reduce(currentSchemaDDL: String, json: String): String = {
    if (currentSchemaDDL == null) {
      SparkSchemaParser.fromJson(json).sql
    } else {
      val currentSchema = DataType.fromDDL(currentSchemaDDL)
      val rowSchema = SparkSchemaParser.fromJson(json)
      mergeSchema(currentSchema, rowSchema).sql
    }
  }

  override def merge(schema1DDL: String, schema2DDL: String): String = if (schema1DDL == null) {
    schema2DDL
  } else if (schema2DDL == null) {
    schema1DDL
  } else {
    val schema1 = DataType.fromDDL(schema1DDL)
    val schema2 = DataType.fromDDL(schema2DDL)
    mergeSchema(schema1, schema2).sql
  }

  private def mergeSchema(schema1: DataType, schema2: DataType): DataType = (schema1, schema2) match {
    case (struct1: StructType, struct2: StructType) => SparkSchemaMerger.mergeStructTypes(struct1, struct2)
    case (array1: ArrayType, array2: ArrayType) => SparkSchemaMerger.mergeArrayTypes(array1, array2)
    case _ => throw new UnsupportedOperationException(s"not able to merge $schema1 with $schema2")
  }

  override def finish(reduction: String): String = reduction

  override def bufferEncoder: Encoder[String] = Encoders.STRING

  override def outputEncoder: Encoder[String] = Encoders.STRING

}
