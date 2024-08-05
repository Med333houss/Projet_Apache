name := "project_scala_spark"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2"
)

fork := true

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-target:jvm-1.8")
