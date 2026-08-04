ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % "3.5.4",
      "org.apache.spark" %% "spark-sql"  % "3.5.4",

      // Configuration
      "com.typesafe" % "config" % "1.4.3"
    )
  )