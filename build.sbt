name := "cetera"

scalaVersion := "2.11.7"
// keeping 2.10 around during transition, once we're happy with 2.11 in prod we can remove it.
crossScalaVersions := Seq("2.10.4", scalaVersion.value)

resolvers ++= Seq(
  "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",
  "Artifactory release" at "https://repo.socrata.com/artifactory/simple/libs-release-local",
  "Artifactory snapshot" at "https://repo.socrata.com/artifactory/simple/libs-snapshot-local",
  "Cloudbees release" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "Cloudbees snapshot" at "https://repository-socrata-oss.forge.cloudbees.com/snapshot",
  Classpaths.sbtPluginReleases,
  Resolver.url("Artifactory ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

val rojomaDependencies = Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.4.1",
  "com.rojoma" %% "simple-arm-v2" % "2.1.0"
)

val socrataDependencies = Seq(
  "com.socrata" %% "socrata-http-client" % "3.5.0",
  "com.socrata" %% "socrata-http-jetty" % "3.5.0",
  "com.socrata" %% "socrata-thirdparty-utils" % "4.0.12",
  "com.socrata" %% "balboa-client" % "0.16.15"
).map { _.excludeAll(ExclusionRule(organization = "com.rojoma")) }

val loggingDependencies = Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)

libraryDependencies ++= rojomaDependencies ++ socrataDependencies ++ loggingDependencies ++ Seq(
  "com.typesafe" % "config" % "1.0.2",
  "org.codehaus.groovy" % "groovy-all" % "2.3.5" % "test",
  "org.elasticsearch" % "elasticsearch" % "1.7.2"
)

initialCommands := "import com.socrata.cetera._"

enablePlugins(sbtbuildinfo.BuildInfoPlugin)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-Yinline-warnings")

// This forks a new JVM because our ES tests leak threads
fork in Test := true
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")

// Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "configs") }
fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d / "configs") }

de.johoop.jacoco4sbt.JacocoPlugin.jacoco.settings

initialize := {
  val jvRequired = "1.8"
  val jvCurrent = sys.props("java.specification.version")
  assert(jvCurrent == jvRequired, s"Unsupported JDK: java.specification.version $jvCurrent != $jvRequired")
}
