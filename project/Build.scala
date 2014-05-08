import sbt._
import Keys._

object Build extends Build {

  lazy val defaultSettings =
    Defaults.defaultSettings ++
      Seq(
        version := "0.1",
        scalaVersion := "2.10.3",
        libraryDependencies ++= Seq(
          "org.testng" % "testng" % "6.8.8" % "test",
          "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
        )
      )

  lazy val root = Project("root",
    file("."),
    settings = defaultSettings
  ).aggregate(drmaaJava, drmaaSlurmJava)

  lazy val drmaaJava = Project("drmaaJava",
    file("drmaaJava"),
    settings = defaultSettings
  )

  lazy val drmaaSlurmJava = Project("drmaaSlurmJava",
    file("drmaaSlurmJava"),
    settings = defaultSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "com.nativelibs4java" % "bridj" % "0.6.2" withJavadoc() withSources())
      )
  ).dependsOn(drmaaJava)

  lazy val samples = Project("samples",
    file("samples"),
    settings = defaultSettings
  ).dependsOn(drmaaJava, drmaaSlurmJava)
}