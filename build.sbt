import xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

def isGithubActions = sys.env.getOrElse("CI", "false") == "true"
def releaseVersion = sys.env.getOrElse("RELEASE_VERSION", "")
def isRelease = releaseVersion != ""
def githubRunNumber = sys.env.getOrElse("GITHUB_RUN_NUMBER", "")
def publishVersion = if (isRelease) releaseVersion else if (isGithubActions) "2.9.0." + githubRunNumber + "-SNAPSHOT" else "0.0.0-LOCAL"

val org = "com.clever-cloud.pulsar4s"
val AkkaStreamVersion = "2.6.20" // compatible with Akka 2.5.x and 2.6.x
val CatsEffectVersion = "3.6.1"
val CirceVersion = "0.14.13"
val CommonsIoVersion = "2.4"
val ExtsVersion = "1.61.1"
val JacksonVersion = "2.18.4"
val Log4jVersion = "2.24.3"
val MonixVersion = "3.4.1"
val PekkoStreamVersion = "1.1.3"
val PlayJsonVersion = "2.10.6"
val PulsarVersion = "4.0.5"
val ReactiveStreamsVersion = "1.0.2"
val FunctionalStreamsVersion = "3.12.0"
val Json4sVersion = "4.0.7"
// Version of Avro4s for Scala 2.X
val Avro4sVersionFor2 = "4.1.2"
// Version of Avro4s for Scala 3.X
val Avro4sVersionFor3 = "5.0.14"
val ScalaVersion = "3.6.4"
val ScalatestVersion = "3.2.19"
val ScalazVersion = "7.2.36"
val Slf4jVersion = "2.0.17"
val SprayJsonVersion = "1.3.6"
val ZIOVersion = "2.1.18"
val ZIOInteropCatsVersion = "23.1.0.5"

lazy val commonScalaVersionSettings = Seq(
  scalaVersion := ScalaVersion,
  crossScalaVersions := Seq("2.12.20", "2.13.16", ScalaVersion)
)

lazy val warnUnusedImport = Seq(
  scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((3, _)) => Seq()
    case _ => Seq("-Ywarn-unused:imports")
  }),
  Compile / console / scalacOptions ~= {
    _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports"))
  },
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
)

lazy val commonSettings = Seq(
  organization := org,
  version := publishVersion,
  resolvers ++= Seq(Resolver.mavenLocal),
  Test / parallelExecution := false,
  Global / parallelExecution := false,
  Global / concurrentRestrictions += Tags.limit(Tags.Test, 1),
  Compile / doc / scalacOptions := (Compile / doc / scalacOptions).value.filter(_ != "-Xfatal-warnings"),
  scalacOptions ++= Seq("-unchecked", "-encoding", "utf8")
    ++ (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq(
        "-deprecation",
      )
      case _ => Seq()
    })
)

lazy val publishSettings = Seq(
  Test / publishArtifact := false,
  pomIncludeRepository := Function.const(false),
  usePgpKeyHex("A7B8F38C536F1DF2"),
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
    "com.sksamuel.exts" %% "exts" % ExtsVersion cross CrossVersion.for3Use2_13,
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

lazy val allSettings = commonScalaVersionSettings ++
  commonJvmSettings ++
  commonSettings ++
  commonDeps ++
  pomSettings ++
  warnUnusedImport ++
  publishSettings

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

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
    pekko_streams,
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
    "com.sksamuel.avro4s" %% "avro4s-core" % (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((3, _)) => Avro4sVersionFor3
      case _ => Avro4sVersionFor2
    })
  ))

lazy val akka_streams = Project("pulsar4s-akka-streams", file("pulsar4s-akka-streams"))
  .dependsOn(core)
  .settings(name := "pulsar4s-akka-streams")
  .settings(allSettings)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-stream" % AkkaStreamVersion
  ))

lazy val pekko_streams = Project("pulsar4s-pekko-streams", file("pulsar4s-pekko-streams"))
  .dependsOn(core)
  .settings(name := "pulsar4s-pekko-streams")
  .settings(allSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-stream" % PekkoStreamVersion
    ),
    // ignore scala-java8-compat issues with scala 2.12
    libraryDependencySchemes ++= (CrossVersion.partialVersion(scalaVersion.value).collect {
      case (2, 12) => "org.scala-lang.modules" %% "scala-java8-compat" % VersionScheme.Always
    })
  )
