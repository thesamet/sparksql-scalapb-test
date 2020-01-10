package myexample

import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scalapb.spark.Implicits._
import scalapb.spark.ProtoSQL

object RunDemo {

  def main(Args: Array[String]): Unit = {
    // Should be placed on all worker machines:
    val inputFile = "/tmp/input.base64.txt"

    val spark = SparkSession.builder().appName("ScalaPB Demo").getOrCreate()

    val sc = spark.sparkContext

    // Converts a base64-encoded line to Person.
    def parseLine(s: String): Person = Person.parseFrom(
      org.apache.commons.codec.binary.Base64.decodeBase64(s)
    )

    val persons: RDD[Person] = sc.textFile(inputFile).map(parseLine)

    val personsDF: DataFrame = ProtoSQL.protoToDataFrame(spark, persons)

    val personsDS1: Dataset[Person] = personsDF.as[Person]

    val personsDS2: Dataset[Person] = spark.createDataset(persons.collect())

    personsDS1.show()

    personsDS2.show()

    personsDF.createOrReplaceTempView("persons")


    spark.sql("SELECT name, age, gender, size(addresses) FROM persons").show()

    spark.sql("SELECT name, age, gender, size(addresses) FROM persons WHERE age > 30")
      .collect
      .foreach(println)

    {
      import scalapb.spark._
      persons.saveAsParquet("/tmp/out.parquet")

      ProtoParquet.loadParquet[Person](spark, "/tmp/out.parquet").collect().foreach(println)
    }
  }
}
