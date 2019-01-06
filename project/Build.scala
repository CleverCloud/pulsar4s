import com.typesafe.sbt.SbtPgp
import com.typesafe.sbt.pgp.PgpKeys
import sbt._
import sbt.plugins.JvmPlugin
import sbt.Keys._

object Build extends AutoPlugin {

  override def trigger = AllRequirements
  override def requires = JvmPlugin

  object autoImport {
    val org                       = "com.sksamuel.pulsar4s"
    val AkkaStreamVersion         = "2.5.19"
    val CatsVersion               = "1.4.0"
    val CirceVersion              = "0.10.1"
    val CommonsIoVersion          = "2.4"
    val ExtsVersion               = "1.60.0"
    val JacksonVersion            = "2.9.7"
    val Log4jVersion              = "2.10.0"
    val PlayJsonVersion           = "2.6.9"
    val PulsarVersion             = "2.2.0"
    val ReactiveStreamsVersion    = "1.0.2"
    val Json4sVersion             = "3.6.2"
    val ScalaVersion              = "2.11.12"
    val ScalatestVersion          = "3.0.5"
    val Slf4jVersion              = "1.7.25"
    val SprayJsonVersion          = "1.3.4"
    val Java8CompatVersion        = "0.8.0"
  }

  import autoImport._

  override def projectSettings = Seq(
    organization := org,
    // a 'compileonly' configuation
    ivyConfigurations += config("compileonly").hide,
    // appending everything from 'compileonly' to unmanagedClasspath
    unmanagedClasspath in Compile ++= update.value.select(configurationFilter("compileonly")),
    scalaVersion := ScalaVersion,
    crossScalaVersions := Seq(ScalaVersion, "2.12.6"),
    publishMavenStyle := true,
    resolvers += Resolver.mavenLocal,
    fork in Test := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    testForkedParallel in Test := false,
    SbtPgp.autoImport.useGpg := true,
    SbtPgp.autoImport.useGpgAgent := true,
    sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild := true,
    credentials += Credentials(Path.userHome / ".sbt" / "pgp.credentials"),
    scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
    scalacOptions += "-target:jvm-1.8",
    javacOptions := Seq("-source", "1.8", "-target", "1.8"),
    libraryDependencies ++= Seq(
      "com.sksamuel.exts" %% "exts"                     % ExtsVersion,
      "org.slf4j"         % "slf4j-api"                 % Slf4jVersion,
      "org.apache.logging.log4j" % "log4j-api"          % Log4jVersion % "test",
      "org.apache.logging.log4j" % "log4j-slf4j-impl"   % Log4jVersion % "test",
      "org.scalatest"                                   %% "scalatest" % ScalatestVersion % "test"
    ),
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    pomExtra := {
      <url>https://github.com/sksamuel/pulsar4s</url>
        <licenses>
          <license>
            <name>Apache 2</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:sksamuel/pulsar4s.git</url>
          <connection>scm:git@github.com:sksamuel/pulsar4s.git</connection>
        </scm>
        <developers>
          <developer>
            <id>sksamuel</id>
            <name>sksamuel</name>
            <url>http://github.com/sksamuel</url>
          </developer>
        </developers>
    }
  )
}
