lazy val root = Project("pulsar4s", file("."))
  .settings(publish := {})
  .settings(publishArtifact := false)
  .settings(name := "pulsar4s")
  .aggregate(
    core
  )

lazy val core = Project("pulsar4s-core", file("pulsar4s-core"))
  .settings(name := "pulsar4s-core")
  .settings(libraryDependencies ++= Seq(
    "org.apache.pulsar" % "pulsar-client" % PulsarVersion,
    "org.apache.pulsar" % "pulsar-common" % PulsarVersion,
    "org.apache.pulsar" % "pulsar-client-admin" % PulsarVersion,
    "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.8.0",
    "com.fasterxml.jackson.core"    % "jackson-core"            % JacksonVersion        % "test",
    "com.fasterxml.jackson.core"    % "jackson-databind"        % JacksonVersion        % "test",
    "com.fasterxml.jackson.module"  %% "jackson-module-scala"   % JacksonVersion        % "test" exclude("org.scala-lang", "scala-library")
  ))