package fr.doba.vincent

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

object SparkReadParquet {

  private val targetDirectory = Paths.get(getClass.getClassLoader.getResource("").toURI).getParent.getParent.toString

  def main(args: Array[String]): Unit = {
    val path = s"$targetDirectory/test-data"

    implicit val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("delta-stream-read")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    spark.read.parquet(path).show(false)

  }

}
