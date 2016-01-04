import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys._

name := "sscheck-examples"

organization := "es.ucm.fdi"

version := "0.0.1"

scalaVersion := "2.10.5"

crossScalaVersions  := Seq("2.10.5")

lazy val sparkVersion = "1.4.1"

lazy val specs2Version = "3.6.4" 

lazy val sscheckVersion = "0.2.0"

// Use `sbt doc` to generate scaladoc, more on chapter 14.8 of "Scala Cookbook"

// show all the warnings: http://stackoverflow.com/questions/9415962/how-to-see-all-the-warnings-in-sbt-0-11
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

// if parallel test execution is not disabled and several test suites using
// SparkContext (even through SharedSparkContext) are running then tests fail randomly
parallelExecution := false

// Could be interesting at some point
// resourceDirectory in Compile := baseDirectory.value / "main/resources"
// resourceDirectory in Test := baseDirectory.value / "main/resources"

// Configure sbt to add the resources path to the eclipse project http://stackoverflow.com/questions/14060131/access-configuration-resources-in-scala-ide
// This is critical so log4j.properties is found by eclipse
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

// Spark 
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// additional libraries: NOTE as we are writing a testing library they should also be available for main
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2"

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-junit" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-mock" % specs2Version

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "[4.0,)"

// FIXME delete libraryDependencies += "org.mockito" % "mockito-core" % "2.0.35-beta"

libraryDependencies += "es.ucm.fdi" %% "sscheck" % sscheckVersion

// sscheck repository
resolvers += Resolver.bintrayRepo("juanrh", "maven")
