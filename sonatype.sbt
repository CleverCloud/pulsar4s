// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("clevercloud", "pulsar4s", "julien.durillon@clever-cloud.com"))
