package myexample

import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.rdd.RDD
import scalapb.spark.Implicits._
import scalapb.spark.ProtoSQL

import java.util.UUID.randomUUID

object RunDemo {

  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()

    val sc = spark.sparkContext

    val personsDF: DataFrame = ProtoSQL.createDataFrame(spark, testData)

    val personsDS1: Dataset[Person] = personsDF.as[Person]

    val personsDS2: Dataset[Person] = spark.createDataset(testData)

    personsDS1.show()

    personsDS2.show()

    personsDF.createOrReplaceTempView("persons")

    spark.sql("SELECT name, age, gender, size(addresses) FROM persons").show()

    spark.sql("SELECT name, age, gender, size(addresses) FROM persons WHERE age > 30")
      .collect
      .foreach(println)
    
    // Convert to Dataframe with column containing binary proto data
    val protosBinary = personsDS2.map({ row: Person => 
        // This is not good in production, caching foibles can cause row ids to mutate. 
        // Doing this here to simulate a unique row identifier. 
        val id: String = randomUUID().toString
        val binaryValue = row.update(_.id := id).toByteArray
        val other_data = "some data"
        (id, other_data, binaryValue)
      })
    val binaryDF = protosBinary.toDF("id", "other_data", "value")
    // This cache is required to prevent the `id` from mutating unexpectedly
    binaryDF.cache
    binaryDF.printSchema

    // parse using dataset api and join back with original data:
    val parsedDF = binaryDF
      .select(F.col("value"))
      .as[Array[Byte]]
      .map(Person.parseFrom(_))
    parsedDF.show(false)
    binaryDF
      .join(
        parsedDF,
        usingColumns=Seq("id"),
        joinType="left"
      )
      .show(false)

    // Parse using ProtoSQL.udf
    // Following https://scalapb.github.io/docs/sparksql#udfs
    /*
    This breaks on databricks 9.1 LTS cluster image (spark 3.1.2, scala 2.12) with:
    ERROR Uncaught throwable from user code: java.lang.VerifyError: class frameless.functions.Spark2_4_LambdaVariable overrides final method genCode.(Lorg/apache/spark/sql/catalyst/expressions/codegen/CodegenContext;)Lorg/apache/spark/sql/catalyst/expressions/codegen/ExprCode;
      at java.lang.ClassLoader.defineClass1(Native Method)
      at java.lang.ClassLoader.defineClass(ClassLoader.java:757)
      at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
      at java.net.URLClassLoader.defineClass(URLClassLoader.java:468)
      at java.net.URLClassLoader.access$100(URLClassLoader.java:74)
      at java.net.URLClassLoader$1.run(URLClassLoader.java:369)
      at java.net.URLClassLoader$1.run(URLClassLoader.java:363)
      at java.security.AccessController.doPrivileged(Native Method)
      at java.net.URLClassLoader.findClass(URLClassLoader.java:362)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:419)
      at com.databricks.backend.daemon.driver.ClassLoaders$LibraryClassLoader.loadClass(ClassLoaders.scala:151)
      at java.lang.ClassLoader.loadClass(ClassLoader.java:352)
      at frameless.functions.FramelessUdf$.apply(Udf.scala:218)
      at scalapb.spark.Udfs.$anonfun$udf$1(Udfs.scala:12)
      at myexample.RunDemo$.main(RunDemo.scala:45)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw$$iw$$iw$$iw$$iw$$iw.<init>(command--1:1)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw$$iw$$iw$$iw$$iw.<init>(command--1:43)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw$$iw$$iw$$iw.<init>(command--1:45)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw$$iw$$iw.<init>(command--1:47)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw$$iw.<init>(command--1:49)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$$iw.<init>(command--1:51)
      at line42e988d4e7ee45329381d02d6281a3d325.$read.<init>(command--1:53)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$.<init>(command--1:57)
      at line42e988d4e7ee45329381d02d6281a3d325.$read$.<clinit>(command--1)
      at line42e988d4e7ee45329381d02d6281a3d325.$eval$.$print$lzycompute(<notebook>:7)
      at line42e988d4e7ee45329381d02d6281a3d325.$eval$.$print(<notebook>:6)
      at line42e988d4e7ee45329381d02d6281a3d325.$eval.$print(<notebook>)
      at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
      at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
      at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
      at java.lang.reflect.Method.invoke(Method.java:498)
      at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:745)
      at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1021)
      at scala.tools.nsc.interpreter.IMain.$anonfun$interpret$1(IMain.scala:574)
      at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:41)
      at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:37)
      at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:41)
      at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:573)
      at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:600)
      at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:570)
      at com.databricks.backend.daemon.driver.DriverILoop.execute(DriverILoop.scala:219)
      at com.databricks.backend.daemon.driver.ScalaDriverLocal.$anonfun$repl$1(ScalaDriverLocal.scala:204)
      at scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.java:23)
      at com.databricks.backend.daemon.driver.DriverLocal$TrapExitInternal$.trapExit(DriverLocal.scala:789)
      at com.databricks.backend.daemon.driver.DriverLocal$TrapExit$.apply(DriverLocal.scala:742)
      at com.databricks.backend.daemon.driver.ScalaDriverLocal.repl(ScalaDriverLocal.scala:204)
      at com.databricks.backend.daemon.driver.DriverLocal.$anonfun$execute$10(DriverLocal.scala:431)
      at com.databricks.logging.UsageLogging.$anonfun$withAttributionContext$1(UsageLogging.scala:239)
      at scala.util.DynamicVariable.withValue(DynamicVariable.scala:62)
      at com.databricks.logging.UsageLogging.withAttributionContext(UsageLogging.scala:234)
      at com.databricks.logging.UsageLogging.withAttributionContext$(UsageLogging.scala:231)
      at com.databricks.backend.daemon.driver.DriverLocal.withAttributionContext(DriverLocal.scala:48)
      at com.databricks.logging.UsageLogging.withAttributionTags(UsageLogging.scala:276)
      at com.databricks.logging.UsageLogging.withAttributionTags$(UsageLogging.scala:269)
      at com.databricks.backend.daemon.driver.DriverLocal.withAttributionTags(DriverLocal.scala:48)
      at com.databricks.backend.daemon.driver.DriverLocal.execute(DriverLocal.scala:408)
      at com.databricks.backend.daemon.driver.DriverWrapper.$anonfun$tryExecutingCommand$1(DriverWrapper.scala:653)
      at scala.util.Try$.apply(Try.scala:213)
      at com.databricks.backend.daemon.driver.DriverWrapper.tryExecutingCommand(DriverWrapper.scala:645)
      at com.databricks.backend.daemon.driver.DriverWrapper.getCommandOutputAndError(DriverWrapper.scala:486)
      at com.databricks.backend.daemon.driver.DriverWrapper.executeCommand(DriverWrapper.scala:598)
      at com.databricks.backend.daemon.driver.DriverWrapper.runInnerLoop(DriverWrapper.scala:391)
      at com.databricks.backend.daemon.driver.DriverWrapper.runInner(DriverWrapper.scala:337)
      at com.databricks.backend.daemon.driver.DriverWrapper.run(DriverWrapper.scala:219)
      at java.lang.Thread.run(Thread.java:748)
    */
    val protoParser = ProtoSQL.udf { bytes: Array[Byte] => Person.parseFrom(bytes) }
    val parsedDF2 = binaryDF.withColumn("parsedValue", protoParser(binaryDF("value")))
    parsedDF2.show(false)
    
    binaryDF.unpersist
  }

  val testData: Seq[Person] = Seq(
    Person().update(
      _.name := "Joe",
      _.age := 32,
      _.gender := Gender.MALE),
    Person().update(
      _.name := "Mark",
      _.age := 21,
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = Some("San Francisco"), street=Some("3rd Street"))
      )),
    Person().update(
      _.name := "Steven",
      _.gender := Gender.MALE,
      _.addresses := Seq(
          Address(city = Some("San Francisco"), street=Some("5th Street")),
          Address(city = Some("Sunnyvale"), street=Some("Wolfe"))
      )),
    Person().update(
      _.name := "Batya",
      _.age := 11,
      _.gender := Gender.FEMALE))
}

case class Payload (
  value: BinaryType
)
