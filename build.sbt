name := "spark-courses"

version := "0.1"

scalaVersion := "2.12.12"

// for spark-fast-tests library
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.0.1"),
  ("org.apache.spark" %% "spark-sql" % "3.0.1"),
  ("org.scalatest" %% "scalatest" % "3.2.2" % Test),
  ("org.mockito" % "mockito-scala-scalatest_2.12" % "1.16.0"),
  ("MrPowers" % "spark-fast-tests" % "0.21.1-s_2.12" % Test)
)
