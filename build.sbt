ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

val AkkaVersion = "2.9.2"
val AlpakkaVersion = "7.0.2"

lazy val root = (project in file("."))
  .settings(
    name := "sensors",
    resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % AlpakkaVersion,
    )
  )
