def isGithubActions = sys.env.getOrElse("CI", "false") == "true"
def releaseVersion = sys.env.getOrElse("RELEASE_VERSION", "")
def isRelease = releaseVersion != ""
def githubRunNumber = sys.env.getOrElse("GITHUB_RUN_NUMBER", "")
def ossrhUsername = sys.env.getOrElse("OSSRH_USERNAME", "")
def ossrhPassword = sys.env.getOrElse("OSSRH_PASSWORD", "")
def publishVersion = if (isRelease) releaseVersion else if (isGithubActions) "2.7.0." + githubRunNumber + "-SNAPSHOT" else "0.0.0-LOCAL"

val org = "com.sksamuel.pulsar4s"
val AkkaStreamVersion = "2.5.32" // compatible with Akka 2.5.x and 2.6.x
val CatsEffectVersion = "2.1.4"
val CirceVersion = "0.12.3"
val CommonsIoVersion = "2.4"
val ExtsVersion = "1.61.1"
val JacksonVersion = "2.9.9"
val Log4jVersion = "2.12.0"
val MonixVersion = "3.1.0"
val PlayJsonVersion = "2.7.4" // compatible with 2.7.x and 2.8.x
val PulsarVersion = "2.7.0"
val ReactiveStreamsVersion = "1.0.2"
val FunctionalStreamsVersion = "2.5.2"
val Json4sVersion = "3.6.10"
val Avro4sVersion = "3.1.0"
val ScalaVersion = "2.12.11"
val ScalatestVersion = "3.2.3"
val Slf4jVersion = "1.7.30"
val SprayJsonVersion = "1.3.6"
val Java8CompatVersion = "0.9.1"
val ZIOVersion = "1.0.4-2"
val ZIOInteropCatsVersion = "2.1.4.1"

lazy val commonScalaVersionSettings = Seq(
  scalaVersion := ScalaVersion,
  crossScalaVersions := Seq("2.12.10", "2.13.5")
)

lazy val warnUnusedImport = Seq(
  scalacOptions ++= Seq("-Ywarn-unused:imports"),
  scalacOptions in(Compile, console) ~= {
    _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports"))
  },
  scalacOptions in(Test, console) := (scalacOptions in(Compile, console)).value,
)

lazy val commonSettings = Seq(
  organization := "com.sksamuel.pulsar4s",
  version := publishVersion,
  resolvers ++= Seq(Resolver.mavenLocal),
  parallelExecution in Test := false,
  scalacOptions in(Compile, doc) := (scalacOptions in(Compile, doc)).value.filter(_ != "-Xfatal-warnings"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-encoding", "utf8")
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := Function.const(false),
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    ossrhUsername,
    ossrhPassword
  ),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isRelease)
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    else
      Some("snapshots" at nexus + "content/repositories/snapshots")
  }
)

lazy val commonJvmSettings = Seq(
  testOptions in Test += {
    val flag = if (isGithubActions) "-oCI" else "-oDF"
    Tests.Argument(TestFrameworks.ScalaTest, flag)
  },
  Test / fork := true,
  Test / javaOptions := Seq("-Xmx3G"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
)

lazy val commonDeps = Seq(
  libraryDependencies ++= Seq(
    "com.sksamuel.exts" %% "exts" % ExtsVersion,
    "org.slf4j" % "slf4j-api" % Slf4jVersion,
    "org.scalatest" %% "scalatest" % ScalatestVersion % "test",
    "org.apache.logging.log4j" % "log4j-api" % Log4jVersion % "test",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % Log4jVersion % "test"
  )
)

lazy val pomSettings = Seq(
  homepage := Some(url("https://github.com/sksamuel/pulsar4s")),
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  scmInfo := Some(ScmInfo(url("https://github.com/sksamuel/pulsar4s"), "scm:git:git@github.com:sksamuel/pulsar4s.git")),
  apiURL := Some(url("http://github.com/sksamuel/pulsar4s/")),
  pomExtra := <developers>
    <developer>
      <id>sksamuel</id>
      <name>Sam Samuel</name>
      <url>https://github.com/sksamuel</url>
    </developer>
  </developers>
)

val travisCreds = Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  sys.env.getOrElse("OSSRH_USERNAME", ""),
  sys.env.getOrElse("OSSRH_PASSWORD", "")
)

val localCreds = Credentials(Path.userHome / ".sbt" / "credentials.sbt")

lazy val credentialSettings = Seq(
  credentials := (if (isGithubActions) Seq(travisCreds) else Seq(localCreds))
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val allSettings = commonScalaVersionSettings ++
  commonJvmSettings ++
  commonSettings ++
  commonDeps ++
  credentialSettings ++
  pomSettings ++
  warnUnusedImport ++
  publishSettings


lazy val root = Project("pulsar4s", file("."))
  .settings(name := "pulsar4s")
  .settings(allSettings)
  .settings(noPublishSettings)
  .aggregate(
    core,
    cats_effect,
    scalaz,
    monix,
    zio,
    jackson,
    circe,
    avro,
    playjson,
    sprayjson,
    json4s,
    akka_streams
  )

lazy val core = Project("pulsar4s-core", file("pulsar4s-core"))
  .settings(name := "pulsar4s-core")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % Java8CompatVersion,
    "org.apache.pulsar" % "pulsar-client" % PulsarVersion
  ))

lazy val cats_effect = Project("pulsar4s-cats-effect", file("pulsar4s-cats-effect"))
  .dependsOn(core)
  .settings(name := "pulsar4s-cats-effect")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % CatsEffectVersion,
    "io.monix" %% "monix" % MonixVersion % Test,
    "dev.zio" %% "zio-interop-cats" % ZIOInteropCatsVersion % Test
  ))

lazy val fs2 = Project("pulsar4s-fs2", file("pulsar4s-fs2"))
  .dependsOn(core)
  .dependsOn(cats_effect)
  .settings(name := "pulsar4s-fs2")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % CatsEffectVersion,
    "co.fs2" %% "fs2-core" % FunctionalStreamsVersion,
    "co.fs2" %% "fs2-reactive-streams" % FunctionalStreamsVersion
  ))

lazy val scalaz = Project("pulsar4s-scalaz", file("pulsar4s-scalaz"))
  .dependsOn(core)
  .settings(name := "pulsar4s-scalaz")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.scalaz" %% "scalaz-core" % "7.2.31",
    "org.scalaz" %% "scalaz-concurrent" % "7.2.31"
  ))

lazy val monix = Project("pulsar4s-monix", file("pulsar4s-monix"))
  .dependsOn(core)
  .settings(name := "pulsar4s-monix")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "io.monix" %% "monix" % MonixVersion
  ))

lazy val zio = Project("pulsar4s-zio", file("pulsar4s-zio"))
  .dependsOn(core)
  .settings(name := "pulsar4s-zio")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "dev.zio" %% "zio" % ZIOVersion
  ))

lazy val jackson = Project("pulsar4s-jackson", file("pulsar4s-jackson"))
  .dependsOn(core)
  .settings(name := "pulsar4s-jackson")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    // For 2.9 releases see https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.9#micro-patches
    "com.fasterxml.jackson.core" % "jackson-core" % JacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % JacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % s"$JacksonVersion.3",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % JacksonVersion
  ))

lazy val circe = Project("pulsar4s-circe", file("pulsar4s-circe"))
  .dependsOn(core)
  .settings(name := "pulsar4s-circe")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion
  ))

lazy val playjson = Project("pulsar4s-play-json", file("pulsar4s-play-json"))
  .dependsOn(core)
  .settings(name := "pulsar4s-play-json")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.play" %% "play-json" % PlayJsonVersion
  ))

lazy val json4s = Project("pulsar4s-json4s", file("pulsar4s-json4s"))
  .dependsOn(core)
  .settings(name := "pulsar4s-json4s")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.json4s" %% "json4s-core" % Json4sVersion,
    "org.json4s" %% "json4s-jackson" % Json4sVersion
  ))

lazy val sprayjson = Project("pulsar4s-spray-json", file("pulsar4s-spray-json"))
  .dependsOn(core)
  .settings(name := "pulsar4s-spray-json")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "io.spray" %% "spray-json" % SprayJsonVersion
  ))

lazy val avro = Project("pulsar4s-avro", file("pulsar4s-avro"))
  .dependsOn(core)
  .settings(name := "pulsar4s-avro")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % Avro4sVersion
  ))

lazy val akka_streams = Project("pulsar4s-akka-streams", file("pulsar4s-akka-streams"))
  .dependsOn(core)
  .settings(name := "pulsar4s-akka-streams")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % AkkaStreamVersion
  ))
