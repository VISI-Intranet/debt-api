ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "debt-db",
    version := "0.0.1",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http" % "10.5.0",
      "com.typesafe.akka" %% "akka-stream" % "2.8.0",
      "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0",
      "de.heikoseeberger" %% "akka-http-json4s" % "1.39.2",
      "org.json4s" %% "json4s-native" % "4.0.6" ,
      "org.json4s" %% "json4s-jackson" % "4.0.6" ,
      "com.typesafe.akka" %% "akka-actor" % "2.8.0" ,
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.1",
      "org.json4s" %% "json4s-ext" % "4.0.6"
    )
  )
