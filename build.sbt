lazy val root = Project("pulsar4s", file("."))
  .settings(publish := {})
  .settings(publishArtifact := false)
  .settings(name := "pulsar4s")
  .aggregate(
    core,
    cats_effect,
    scalaz,
    monix,
    jackson,
    circe,
    playjson,
    sprayjson,
    json4s,
    akka_streams
  )

lazy val core = Project("pulsar4s-core", file("pulsar4s-core"))
  .settings(name := "pulsar4s-core")
  .settings(libraryDependencies ++= Seq(
    "org.scala-lang.modules"        %% "scala-java8-compat"         % Java8CompatVersion,
    "org.apache.pulsar"             %  "pulsar-client"              % PulsarVersion
  ))

lazy val cats_effect = Project("pulsar4s-cats-effect", file("pulsar4s-cats-effect"))
  .settings(name := "pulsar4s-cats-effect")
  .settings(libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "0.10.1"
  ))
  .dependsOn(core)

lazy val scalaz = Project("pulsar4s-scalaz", file("pulsar4s-scalaz"))
  .settings(name := "pulsar4s-scalaz")
  .settings(libraryDependencies ++= Seq(
    "org.scalaz" %% "scalaz-core" % "7.2.27",
    "org.scalaz" %% "scalaz-concurrent" % "7.2.27"
  ))
  .dependsOn(core)

lazy val monix = Project("pulsar4s-monix", file("pulsar4s-monix"))
  .settings(name := "pulsar4s-monix")
  .settings(libraryDependencies ++= Seq(
    "io.monix" %% "monix" % "2.3.3"
  ))
  .dependsOn(core)

lazy val jackson = Project("pulsar4s-jackson", file("pulsar4s-jackson"))
  .settings(name := "pulsar4s-jackson")
  .settings(libraryDependencies ++= Seq(
    "com.fasterxml.jackson.core"    % "jackson-core"          % JacksonVersion,
    "com.fasterxml.jackson.core"    % "jackson-annotations"   % JacksonVersion,
    "com.fasterxml.jackson.core"    % "jackson-databind"      % JacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala"  % JacksonVersion
  ))
  .dependsOn(core)

lazy val circe = Project("pulsar4s-circe", file("pulsar4s-circe"))
  .settings(name := "pulsar4s-circe")
  .settings(libraryDependencies ++= Seq(
    "io.circe" %% "circe-core"     % CirceVersion,
    "io.circe" %% "circe-generic"  % CirceVersion,
    "io.circe" %% "circe-parser"   % CirceVersion
  ))
  .dependsOn(core)

lazy val playjson = Project("pulsar4s-play-json", file("pulsar4s-play-json"))
  .settings(name := "pulsar4s-play-json")
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.play" %% "play-json" % PlayJsonVersion
  ))
  .dependsOn(core)

lazy val json4s = Project("pulsar4s-json4s", file("pulsar4s-json4s"))
  .settings(name := "pulsar4s-json4s")
  .settings(libraryDependencies ++= Seq(
    "org.json4s" %% "json4s-core"     % Json4sVersion,
    "org.json4s" %% "json4s-jackson"  % Json4sVersion
  ))
  .dependsOn(core)

lazy val sprayjson = Project("pulsar4s-spray-json", file("pulsar4s-spray-json"))
  .settings(name := "pulsar4s-spray-json")
  .settings(libraryDependencies ++= Seq(
    "io.spray" %% "spray-json" % SprayJsonVersion
  ))
  .dependsOn(core)

lazy val akka_streams = Project("pulsar4s-akka-streams", file("pulsar4s-akka-streams"))
  .settings(name := "pulsar4s-akka-streams")
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % AkkaStreamVersion
  ))
  .dependsOn(core)