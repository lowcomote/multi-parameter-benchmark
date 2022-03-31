ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "CountWord"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "ParametrizableCountWord.jar" }
