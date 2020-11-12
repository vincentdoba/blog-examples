package fr.doba.vincent

object SerializedToInteger extends CommonCode {

  val zero: Integer = Integer.valueOf(0)

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    for (i <- 1 to 20) {
      spark.time(
        read("/tmp/bigfile.csv")
          .as[(Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer)]
          .map(value => if (value._5 == null) zero else value._5)
          .write
          .mode("overwrite")
          .parquet("/tmp/serializedToInteger")
      )
    }
  }


}
