lazy val root = (project in file("."))
  .settings(
    organization := "fr.doba.vincent",
    name := "testcontainer-flyway-example",
    version := "1.0",
    scalaVersion := "2.13.5",
    libraryDependencies ++= Seq(
      "org.flywaydb" % "flyway-core" % "7.7.0", 
      "org.postgresql" % "postgresql" % "42.2.19",
      "org.scalatest" %% "scalatest" % "3.2.5" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.39.3" % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.39.3" % Test
    ),
    Test / fork := true
  )
