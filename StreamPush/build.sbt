
ThisBuild / version := "0"

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "StreamPush"
  )
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.23.1"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.23.1"
// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api-scala
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0"
// https://mvnrepository.com/artifact/com.google.code.gson/gson
libraryDependencies += "com.google.code.gson" % "gson" % "2.11.0"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.4.9"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "2.4.9"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "2.4.9"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.2.0"

// https://mvnrepository.com/artifact/org.apache.hbase.connectors.spark/hbase-spark
libraryDependencies += "org.apache.hbase.connectors.spark" % "hbase-spark" % "1.0.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

// https://mvnrepository.com/artifact/org.scala-lang/scala-library
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2" % "provided"


// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % "3.1.2"
//// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
//libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.2"