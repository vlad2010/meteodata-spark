name := "Meteo Data Analyser"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.6.2" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.6.2" % "provided",
)