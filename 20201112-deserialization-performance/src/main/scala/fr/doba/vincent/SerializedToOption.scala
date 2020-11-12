package fr.doba.vincent

object SerializedToOption extends CommonCode {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    for (i <- 1 to 20) {
      spark.time(
        read("/tmp/bigfile.csv")
          .as[(Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int], Option[Int])]
          .map(_._5.getOrElse(0))
          .write
          .mode("overwrite")
          .parquet("/tmp/serializedToOption")
      )
    }


  }
}
