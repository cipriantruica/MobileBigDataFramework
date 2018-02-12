name := "FullPipelineCluster"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"
