lazy val root = (project in file("."))
  .settings(
    organization := "fr.doba.vincent",
    name := "json-schema-aggregator",
    version := "1.0",
    scalaVersion := "2.12.15",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.1.2",
      "org.scalactic" %% "scalactic" % "3.2.10" % "test",
      "org.scalatest" %% "scalatest" % "3.2.10" % "test"
    )
  )
