package fr.doba.vincent.aggregator

import org.apache.spark.sql.types._

object SparkSchemaMerger {

  def mergeStructTypes(left: StructType, right: StructType): StructType = {
    new StructType(
      (left ++ right)
        .groupBy(_.name)
        .map(it => {
          mergeFields(it._1, it._2.head.dataType, it._2.last.dataType)
        })
        .toArray
    )
  }

  def mergeArrayTypes(left: ArrayType, right: ArrayType): ArrayType = ArrayType(
    mergeFields("not_used", left.elementType, right.elementType).dataType
  )

  private def mergeFields(name: String, left: DataType, right: DataType): StructField = (left, right) match {
    case (a: StructType, b: StructType) => StructField(name, mergeStructTypes(a, b))
    case (a: ArrayType, b: ArrayType) => StructField(name, mergeArrayTypes(a, b))
    case (a, _: StringType) => StructField(name, a)
    case (_: StringType, b) => StructField(name, b)
    case _ => StructField(name, left)
  }

}
