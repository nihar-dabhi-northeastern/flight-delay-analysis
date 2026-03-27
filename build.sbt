ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "edu.neu.csye7200"

val sparkVersion = "3.5.0"
val kafkaVersion = "3.6.0"

lazy val root = (project in file("."))
  .settings(
    name := "flight-delay-analytics",

    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core"             % sparkVersion,
      "org.apache.spark" %% "spark-sql"              % sparkVersion,
      "org.apache.spark" %% "spark-mllib"            % sparkVersion,
      // Spark Kafka integration
      "org.apache.spark" %% "spark-sql-kafka-0-10"   % sparkVersion,
      // Kafka producer client
      "org.apache.kafka"  % "kafka-clients"           % kafkaVersion,
      // PostgreSQL JDBC driver
      "org.postgresql"    % "postgresql"              % "42.7.1",
      // Testing
      "org.scalatest"    %% "scalatest"               % "3.2.17" % Test
    ),

    // Merge strategy for sbt-assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },

    // Fork JVM with enough memory for Spark
    Compile / run / fork := true,
    javaOptions ++= Seq(
      "-Xms512m",
      "-Xmx2g"
    )
  )