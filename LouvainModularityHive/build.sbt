name := "Louvain Modularity"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

// http://stackoverflow.com/questions/28612837/spark-classnotfoundexception-when-running-hello-world-example-in-scala-2-11
fork := true

