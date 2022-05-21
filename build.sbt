// SparkSQL can work with a Spark built with Scala 2.11 too.

scalaVersion := "2.12.10"

version := "1.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",
  "com.thesamet.scalapb" %% "sparksql32-scalapb0_11" % "1.0.0"
)

// Hadoop contains an old protobuf runtime that is not binary compatible
// with 3.0.0.  We shaded ours to prevent runtime issues.
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll,
  ShadeRule.rename("scala.collection.compat.**" -> "scalacompat.@1").inAll,
  ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

