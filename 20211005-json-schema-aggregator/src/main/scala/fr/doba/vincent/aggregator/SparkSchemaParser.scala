package fr.doba.vincent.aggregator

import com.fasterxml.jackson.core.JsonToken._
import com.fasterxml.jackson.core.{JsonFactoryBuilder, JsonParser}
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.json.JacksonUtils.nextUntil
import org.apache.spark.sql.catalyst.json.JsonInferSchema
import org.apache.spark.sql.types._

object SparkSchemaParser {

  def fromJson(json: String): DataType = {
    val parser = new JsonFactoryBuilder().build().createParser(json)
    try {
      parser.nextToken()
      // To match with schema inference from JSON datasource.
      inferField(parser) match {
        case st: StructType =>
          canonicalizeType(st).getOrElse(StructType(Nil))
        case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
          canonicalizeType(at.elementType)
            .map(ArrayType(_, containsNull = at.containsNull))
            .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
        case other: DataType =>
          canonicalizeType(other).getOrElse(StringType)
      }
    } finally {
      parser.close()
    }
  }

  private def inferField(parser: JsonParser): DataType = {
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING => StringType

      case START_OBJECT =>
        val builder = Array.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(
            parser.getCurrentName,
            inferField(parser),
            nullable = true)
        }
        val fields: Array[StructField] = builder.result()
        // Note: other code relies on this sorting for correctness, so don't remove it!
        java.util.Arrays.sort(fields, JsonInferSchema.structFieldComparator)
        StructType(fields)

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = JsonInferSchema.compatibleType(
            elementType, inferField(parser))
        }

        ArrayType(elementType)

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            if (Math.max(v.precision(), v.scale()) <= DecimalType.MAX_PRECISION) {
              DecimalType(Math.max(v.precision(), v.scale()), v.scale())
            } else {
              DoubleType
            }
          case FLOAT | DOUBLE =>
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType

      case _ =>
        throw new SparkException("Malformed JSON")
    }
  }

  private def canonicalizeType(tpe: DataType): Option[DataType] = tpe match {
    case at: ArrayType =>
      canonicalizeType(at.elementType)
        .map(t => at.copy(elementType = t))

    case StructType(fields) =>
      val canonicalFields = fields.filter(_.name.nonEmpty).flatMap { f =>
        canonicalizeType(f.dataType)
          .map(t => f.copy(dataType = t))
      }

      if (canonicalFields.isEmpty) {
        None
      } else {
        Some(StructType(canonicalFields))
      }

    case NullType => Some(StringType)

    case other => Some(other)
  }

}
