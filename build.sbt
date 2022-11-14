
name := "BatchETL2"

version := "0.1"

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "All Spark Repository -> bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
)


libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.hadoop" % "hadoop-common" % "2.7.0",
  "org.apache.spark" % "spark-hive_2.11" % "2.2.0",
  "org.apache.spark" % "spark-yarn_2.11" % "2.2.0",
  "org.apache.kudu" % "kudu-spark2_2.11" % "1.5.0"
)

libraryDependencies ++= Seq(

  "com.typesafe" % "config" % "1.3.2",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test"
)

libraryDependencies ++= Seq(

  "io.prometheus" % "simpleclient" % "0.1.0",
  "io.prometheus" % "simpleclient_common" % "0.1.0",
  "io.prometheus" % "simpleclient_hotspot" % "0.1.0",
  "io.prometheus" % "simpleclient_pushgateway" % "0.1.0",
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {} 
