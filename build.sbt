def isGithubActions = sys.env.getOrElse("CI", "false") == "true"
def releaseVersion = sys.env.getOrElse("RELEASE_VERSION", "")
def isRelease = releaseVersion != ""
def githubRunNumber = sys.env.getOrElse("GITHUB_RUN_NUMBER", "")
def ossrhUsername = sys.env.getOrElse("OSSRH_USERNAME", "")
def ossrhPassword = sys.env.getOrElse("OSSRH_PASSWORD", "")
def publishVersion = if (isRelease) releaseVersion else if (isGithubActions) "2.8.2." + githubRunNumber + "-SNAPSHOT" else "0.0.0-LOCAL"

val org = "com.clever-cloud.pulsar4s"
val AkkaStreamVersion = "2.6.19" // compatible with Akka 2.5.x and 2.6.x
val CatsEffectVersion = "3.3.14"
val CirceVersion = "0.14.2"
val CommonsIoVersion = "2.4"
val ExtsVersion = "1.61.1"
val JacksonVersion = "2.13.3"
val Log4jVersion = "2.17.2"
val MonixVersion = "3.4.1"
val PlayJsonVersion = "2.8.2" // compatible with 2.7.x and 2.8.x
val PulsarVersion = "2.10.2"
val ReactiveStreamsVersion = "1.0.2"
val FunctionalStreamsVersion = "3.2.14"
val Json4sVersion = "4.0.5"
val Avro4sVersion = "4.0.13"
val ScalaVersion = "2.13.8"
val ScalatestVersion = "3.2.13"
val ScalazVersion = "7.2.34"
val Slf4jVersion = "1.7.36"
val SprayJsonVersion = "1.3.6"
val ZIOVersion = "1.0.16"
val ZIOInteropCatsVersion = "3.2.9.1"

lazy val commonScalaVersionSettings = Seq(
  scalaVersion := ScalaVersion,
  crossScalaVersions := Seq("2.12.16", "2.13.8")
)

lazy val warnUnusedImport = Seq(
  scalacOptions ++= Seq("-Ywarn-unused:imports"),
  Compile / console / scalacOptions ~= {
    _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports"))
  },
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
)

lazy val commonSettings = Seq(
  organization := "com.clever-cloud.pulsar4s",
  version := publishVersion,
  resolvers ++= Seq(Resolver.mavenLocal),
  Test / parallelExecution := false,
  Global / parallelExecution := false,
  Global / concurrentRestrictions += Tags.limit(Tags.Test, 1),
  Compile / doc / scalacOptions := (Compile / doc / scalacOptions).value.filter(_ != "-Xfatal-warnings"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-encoding", "utf8")
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := Function.const(false),
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "s01.oss.sonatype.org",
    ossrhUsername,
    ossrhPassword
  ),
  publishTo := {
    val nexus = "https://s01.oss.sonatype.org/"
    if (isRelease)
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
    else
      Some("snapshots" at nexus + "content/repositories/snapshots")
  }
)

lazy val commonJvmSettings = Seq(
  Test / testOptions += {
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
  homepage := Some(url("https://github.com/CleverCloud/pulsar4s")),
  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  scmInfo := Some(ScmInfo(url("https://github.com/CleverCloud/pulsar4s"), "scm:git:git@github.com:CleverCloud/pulsar4s.git")),
  apiURL := Some(url("http://github.com/CleverCloud/pulsar4s/")),
  pomExtra := <developers>
    <developer>
      <id>sksamuel</id>
      <name>Sam Samuel</name>
      <url>https://github.com/sksamuel</url>
    </developer>
    <developer>
      <id>judu</id>
      <name>Julien Durillon</name>
      <url>https://github.com/judu</url>
    </developer>
  </developers>
)

val travisCreds = Credentials(
  "Sonatype Nexus Repository Manager",
  "s01.oss.sonatype.org",
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
    akka_streams,
    avro,
    cats_effect,
    circe,
    core,
    fs2,
    jackson,
    json4s,
    monix,
    playjson,
    scalaz,
    sprayjson,
    zio,
  )

lazy val core = Project("pulsar4s-core", file("pulsar4s-core"))
  .settings(name := "pulsar4s-core")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-java8-compat" % {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) => "1.0.2"
        case Some((2, n)) if n >= 13 => "1.0.2"
        case _ => "0.8.0"
      }
    },
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
  .settings(libraryDependencies ++= {
    Seq(
      "org.scalaz" %% "scalaz-core" % ScalazVersion,
      "org.scalaz" %% "scalaz-concurrent" % ScalazVersion,
    )
  })

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
    "com.fasterxml.jackson.core" % "jackson-databind" % JacksonVersion,
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
