name := "monitoring_queries"

version := "1.0"
scalaVersion := "2.10.4"

libraryDependencies +=  "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"
artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name +  "." + artifact.extension
}
