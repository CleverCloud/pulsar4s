lazy val root = Project("pulsar4s", file("."))
  .settings(publish := {})
  .settings(publishArtifact := false)
  .settings(name := "pulsar4s")
  .aggregate(
    core,
    cats_effect,
    streams,
    jackson,
    circe
  )

lazy val core = Project("pulsar4s-core", file("pulsar4s-core"))
  .settings(name := "pulsar4s-core")
  .settings(libraryDependencies ++= Seq(
    "org.apache.pulsar" % "pulsar-client" % PulsarVersion,
    "org.apache.pulsar" % "pulsar-common" % PulsarVersion,
    "org.apache.pulsar" % "pulsar-client-admin" % PulsarVersion,
    "org.scala-lang.modules" %% "scala-java8-compat" % Java8CompatVersion,
    "com.fasterxml.jackson.core"    % "jackson-core"            % JacksonVersion        % "test",
    "com.fasterxml.jackson.core"    % "jackson-databind"        % JacksonVersion        % "test",
    "com.fasterxml.jackson.module"  %% "jackson-module-scala"   % JacksonVersion        % "test" exclude("org.scala-lang", "scala-library")
  ))

lazy val cats_effect = Project("pulsar4s-cats-effect", file("pulsar4s-cats-effect"))
  .settings(name := "pulsar4s-cats-effect")
  .settings(libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "0.8"
  ))
  .dependsOn(core)

lazy val streams = Project("pulsar4s-streams", file("pulsar4s-streams"))
  .settings(name := "pulsar4s-streams")
  .settings(libraryDependencies ++= Seq(
    "org.reactivestreams" % "reactive-streams"      % ReactiveStreamsVersion,
    "org.reactivestreams" % "reactive-streams-tck"  % ReactiveStreamsVersion % "test"
  ))
  .dependsOn(core)

lazy val jackson = Project("pulsar4s-jackson", file("pulsar4s-jackson"))
  .settings(name := "pulsar4s-jackson")
  .settings(libraryDependencies ++= Seq(
    "com.fasterxml.jackson.core"    % "jackson-core"          % JacksonVersion,
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