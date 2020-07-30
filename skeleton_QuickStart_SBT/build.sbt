name := "skeleton_QuickStart_SBT"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core"  % "2.2.1",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql"   % "2.2.1"
)

