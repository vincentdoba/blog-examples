package fr.doba.vincent

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CommonCode {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("delta-stream-read")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "localhost")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()

  val schema: StructType = StructType(Seq(
    StructField("id", IntegerType),
    StructField("value1", IntegerType),
    StructField("value2", IntegerType),
    StructField("value3", IntegerType),
    StructField("value4", IntegerType),
    StructField("value5", IntegerType),
    StructField("value6", IntegerType),
    StructField("value7", IntegerType),
    StructField("value8", IntegerType),
    StructField("value9", IntegerType),
    StructField("value10", IntegerType)
  ))

  def read(path: String): DataFrame = spark.read.format("csv")
    .schema(schema)
    .load(path)
    .select("value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10")

}
