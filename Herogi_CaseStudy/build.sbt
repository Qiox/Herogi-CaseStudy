scalaVersion := "2.12.12"
scalacOptions += "-language:higherKinds"
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3" cross CrossVersion.binary)

scalacOptions += "-Ydelambdafy:inline"
libraryDependencies += "org.scastie" %% "runtime-scala" % "1.0.0-SNAPSHOT"
turbo := true
useSuperShell := false
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0"
)