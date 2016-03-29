import com.trueaccord.scalapb.{ScalaPbPlugin => PB}

// SparkSQL can work with a Spark built with Scala 2.11 too.
// scalaVersion := "2.11.7"

PB.protobufSettings

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided",
  "com.trueaccord.scalapb" %% "sparksql-scalapb" % "0.1.2"
)

// Hadoop contains an old protobuf runtime that is not binary compatible
// with 3.0.0.  We shared ours to prevent runtime issues.
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)
