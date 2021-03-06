lazy val root = (project in file("."))
  .settings(
    organization := "fr.doba.vincent",
    name := "spark-aggregator-window-duplicates",
    version := "1.0",
    scalaVersion := "2.12.12",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.0.1"
    ),
    parallelExecution in Test := true
  )
