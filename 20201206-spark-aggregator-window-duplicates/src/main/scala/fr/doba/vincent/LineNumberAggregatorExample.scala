package fr.doba.vincent

import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

object LineNumberAggregatorExample {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("delta-stream-read")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val line_number = udaf(LineNumber).asNondeterministic()

    import spark.implicits._

    val dataframeWithoutDuplicates = Seq(
      "2020-12-01",
      "2020-12-02",
      "2020-12-03",
      "2020-12-04"
    ).toDF("date")

    println("=== Input dataframe without duplicates ===")

    dataframeWithoutDuplicates.show(false)

    println("=== Aggregator result with input dataframe without duplicates ===")

    dataframeWithoutDuplicates
      .withColumn("line_number", line_number().over(Window.orderBy("date")))
      .show(false)

    val dataframeWithDuplicates = Seq(
      "2020-12-01",
      "2020-12-01",
      "2020-12-02",
      "2020-12-02"
    ).toDF("date")

    println("=== Input dataframe without duplicates ===")

    dataframeWithDuplicates.show(false)

    println("=== Aggregator result with input dataframe without duplicates ===")

    dataframeWithDuplicates
      .withColumn("line_number", line_number().over(Window.orderBy("date")))
      .show(false)

  }

}

object LineNumber extends Aggregator[Int, Int, Int] {
  override def zero: Int = 0

  override def reduce(b: Int, a: Int): Int = b + 1

  override def merge(b1: Int, b2: Int): Int = b1 + b2

  override def finish(reduction: Int): Int = reduction

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}
