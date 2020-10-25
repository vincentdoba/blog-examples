package fr.doba.vincent

import java.io.File
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkWriteParquet {

  private val targetDirectory = Paths.get(getClass.getClassLoader.getResource("").toURI).getParent.getParent.toString

  def main(args: Array[String]): Unit = {
    val path = s"$targetDirectory/test-data"
    FileUtils.deleteQuietly(new File(s"$targetDirectory/test-data"))

    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("delta-stream-read")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.default.parallelism", "1")
      .getOrCreate()

    import spark.implicits._

    Seq((1, "value11")).toDF("id", "column1").write.mode(SaveMode.Overwrite).parquet(path)
    Seq((2, "value12", "value22")).toDF("id", "column1", "column2").write.mode(SaveMode.Append).parquet(path)

  }

}
