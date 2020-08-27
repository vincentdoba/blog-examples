package fr.doba.vincent

import java.io.File
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkReadBatch {

  private val targetDirectory = Paths.get(getClass.getClassLoader.getResource("").toURI).getParent.getParent.toString

  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File(s"$targetDirectory/test-data"))

    val path = s"$targetDirectory/test-data/input"
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("delta-stream-read")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    Seq(1, 2, 3).toDF("value").write.mode(SaveMode.Append).parquet(path)

    readAndDisplay(path)

    Seq(4, 5, 6).toDF("value").write.mode(SaveMode.Append).parquet(path)

    readAndDisplay(path)

  }

  def readAndDisplay(path: String)(implicit spark: SparkSession): Unit = {
    spark
      .read
      .parquet(path)
      .collect()
      .map(_.get(0))
      .foreach(println)
  }

}
