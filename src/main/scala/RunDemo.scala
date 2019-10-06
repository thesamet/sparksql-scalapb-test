import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scalapb.spark._

object RunDemo extends App {
  // Register our UDTs to avoid "<none> is not a term" error:
  com.example.protos.demo.DemoProtoUdt.register()

  // Should be placed on all worker machines:
  val inputFile = "/tmp/input.base64.txt"
  val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext

  // Converts a base64-encoded line to Person.
  def parseLine(s: String): Person = Person.parseFrom(
    org.apache.commons.codec.binary.Base64.decodeBase64(s)
  )

  val persons: RDD[Person] = sc.textFile(inputFile).map(parseLine)

  // the above import scalapb.spark._ is needed for the following to work:
  val personsDF: DataFrame = persons.toDataFrame(spark)

  val personsDS1: Dataset[Person] = spark.createDataset(persons.collect())

  val personsDS2: Dataset[Person] = persons.map(_.toByteArray).toDF.map(r => Person.parseFrom(r.getAs[Array[Byte]]("value")))

  personsDS1.show()

  personsDS2.show()

  personsDF.createOrReplaceTempView("persons")


  spark.sql("SELECT name, age, gender, size(addresses) FROM persons").show()

  spark.sql("SELECT name, age, gender, size(addresses) FROM persons WHERE age > 30")
    .collect
    .foreach(println)

  persons.saveAsParquet("/tmp/out.parquet")

  ProtoParquet.loadParquet[Person](spark, "/tmp/out.parquet").collect().foreach(println)
}
