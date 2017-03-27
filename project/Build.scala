import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys.buildInfoPackage
import sbtbuildinfo.BuildInfoPlugin
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy

object CeteraBuild extends Build {
  val Name = "come.socrata.cetera"

  lazy val commonSettings = Seq(
    scalaVersion := "2.11.7",
    crossScalaVersions := Seq(scalaVersion.value),

    scalacOptions ++= Seq("-Yinline-warnings"),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    initialize := {
      val jvRequired = "1.8"
      val jvCurrent = sys.props("java.specification.version")
      assert(jvCurrent == jvRequired, s"Unsupported JDK: java.specification.version $jvCurrent != $jvRequired")
    },

    fork in Test := true,
    testOptions in Test += Tests.Argument("-oDF"),
    resolvers ++= Deps.resolverList
  )

  lazy val build = Project(
    "cetera",
    file("."),
    settings = commonSettings
  )
    .aggregate(ceteraHttp)

  val dependenciesSnippet = SettingKey[xml.NodeSeq]("dependencies-snippet")

  lazy val ceteraHttp = Project(
    "cetera-http",
    file("./cetera-http/"),
    settings = commonSettings ++ de.johoop.jacoco4sbt.JacocoPlugin.jacoco.settings ++ Seq(
      // Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
      fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") },
      fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") },
      buildInfoPackage := "com.socrata.cetera",
      libraryDependencies ++= Deps.http,
      dependenciesSnippet := <xml:group/>,
      ivyXML <<= dependenciesSnippet { snippet =>
        <dependencies>
          {snippet.toList}
        <exclude org="commons-logging" module="commons-logging-api"/>
        <exclude org="commons-logging" module="commons-logging"/>
        </dependencies>
      },
      assemblyMergeStrategy in assembly := {
        case "META-INF/io.netty.versions.properties" => MergeStrategy.last
        case other => MergeStrategy.defaultMergeStrategy(other)
      }
    )
  ).disablePlugins(JmhPlugin).enablePlugins(BuildInfoPlugin)
}

object Deps {
  lazy val resolverList = Seq(
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",
    "Artifactory release" at "https://repo.socrata.com/artifactory/simple/libs-release-local",
    "Artifactory snapshot" at "https://repo.socrata.com/artifactory/simple/libs-snapshot-local",
    "Cloudbees release" at "https://repository-socrata-oss.forge.cloudbees.com/release",
    "Cloudbees snapshot" at "https://repository-socrata-oss.forge.cloudbees.com/snapshot",
    Resolver.url("Artifactory ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns),
    Classpaths.sbtPluginReleases
  )

  lazy val http = common ++ logging ++ rojoma ++ socrata ++ testing

  lazy val common = Seq(
    "com.typesafe" % "config" % "1.0.2",
    "org.apache.lucene" % "lucene-expressions" % "4.10.3" % "test",
    "org.codehaus.groovy" % "groovy-all" % "2.3.5" % "test",
    "org.elasticsearch.client" % "transport" % "5.2.2",
    "org.mock-server" % "mockserver-maven-plugin" % "3.10.1" % "test"
  )

  // NOTE: w/ the upgrade to ES 5, we had to introduce the log4j dependencies in order for tests to run
  // TODO: figure out if we can explicitly configure the logger at test time and remove these deps
  lazy val logging = Seq(
    "org.slf4j" % "slf4j-log4j12" % "1.7.10",
    "io.airbrake" % "airbrake-java" % "2.2.8" exclude("commons-logging", "commons-logging")
  )

  lazy val rojoma = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.4.1",
    "com.rojoma" %% "simple-arm-v2" % "2.1.0"
  )

  lazy val socrata = Seq(
    "com.socrata" %% "socrata-http-client" % "3.5.0",
    "com.socrata" %% "socrata-http-jetty" % "3.5.0",
    "com.socrata" %% "socrata-thirdparty-utils" % "4.0.12",
    "com.socrata" %% "balboa-client" % "0.16.15"
  ).map { _.excludeAll(ExclusionRule(organization = "com.rojoma")) }

  lazy val testing = Seq(
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
    "org.springframework" % "spring-test" % "3.2.10.RELEASE" % "test",
    "org.apache.logging.log4j" % "log4j-api" % "2.7",
    "org.apache.logging.log4j" % "log4j-core" % "2.7"
  )
}
