lazy val root = (project in file("."))
  .settings(
    organization := "fr.doba.vincent",
    name := "mock-amazon-sqs-server",
    version := "1.0",
    scalaVersion := "2.13.3",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.0" % Test,
      "com.github.tomakehurst" % "wiremock-jre8" % "2.27.1" % Test,
      "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.820" % Test
    )
  )
