package fr.doba.vincent

import java.io.{BufferedWriter, File, FileWriter}

object GenerateData {

  def main(args: Array[String]): Unit = {
    val file = new File("/tmp/bigfile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    for (i <- 1 to 50000000) {
      val stringToWrite = if (i % 100 == 0) s"$i,,,,,,,,,,\n" else s"$i,$i,$i,$i,$i,$i,$i,$i,$i,$i,$i,$i\n"
      bw.write(stringToWrite)
    }
    bw.close()
  }
}