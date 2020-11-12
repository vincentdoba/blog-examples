package fr.doba.vincent

object NotSerialized extends CommonCode {

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 20) {
      spark.time(
        read("/tmp/bigfile.csv")
          .na.fill(0, Seq("value5"))
          .select("value5")
          .write
          .mode("overwrite")
          .parquet("/tmp/notSerialized")
      )
    }
  }


}
