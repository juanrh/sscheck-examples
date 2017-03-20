import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys._

name := "sscheck-examples"

organization := "es.ucm.fdi"

version := "0.0.3"

scalaVersion := "2.10.6"

crossScalaVersions  := Seq("2.10.6")

lazy val sparkVersion = "1.6.2"

lazy val specs2Version = "3.8.4" 

lazy val sscheckVersion = "0.3.2-SNAPSHOT" // "0.3.1" //

// Use `sbt doc` to generate scaladoc, more on chapter 14.8 of "Scala Cookbook"

// show all the warnings: http://stackoverflow.com/questions/9415962/how-to-see-all-the-warnings-in-sbt-0-11
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

// if parallel test execution is not disabled and several test suites using
// SparkContext (even through SharedSparkContext) are running then tests fail randomly
parallelExecution := false

// to avoid ClassNotFoundException http://stackoverflow.com/questions/23251001/sbt-test-setup-throws-java-lang-classnotfoundexception when running in sbt 
// this makes the test run in separate JVM http://www.scala-sbt.org/0.12.3/docs/Detailed-Topics/Forking.html
fork := true

// Could be interesting at some point
// resourceDirectory in Compile := baseDirectory.value / "main/resources"
// resourceDirectory in Test := baseDirectory.value / "main/resources"

// Configure sbt to add the resources path to the eclipse project http://stackoverflow.com/questions/14060131/access-configuration-resources-in-scala-ide
// This is critical so log4j.properties is found by eclipse
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// Spark 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// Twitter4j
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"

// Test dependencies
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version % "test"

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % specs2Version % "test"

libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % specs2Version % "test"

libraryDependencies += "org.specs2" %% "specs2-junit" % specs2Version % "test"

libraryDependencies += "org.mockito" % "mockito-all" % "1.9.5" % "test"

libraryDependencies += "es.ucm.fdi" %% "sscheck" % sscheckVersion % "test"

resolvers ++= Seq(
  "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
)

// sscheck repository
resolvers += Resolver.bintrayRepo("juanrh", "maven")
