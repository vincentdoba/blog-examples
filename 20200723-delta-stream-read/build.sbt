lazy val root = (project in file("."))
  .settings(
    organization := "fr.doba.vincent",
    name := "delta-stream-read",
    version := "1.0",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.0.0",
      "io.delta" %% "delta-core" % "0.7.0"
    )
  )
