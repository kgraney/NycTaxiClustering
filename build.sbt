name := "nyctaxi"

version := "1.0"

//scalaVersion := "2.11.7"
scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0"
//libraryDependencies += "org.apache.spark" %% "spark-ml" % "1.5.0"
//libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

mainClass in Compile := Some("Main")
assemblyJarName in assembly := "nyc_taxi.jar"