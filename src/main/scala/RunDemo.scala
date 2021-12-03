package myexample

import com.example.protos.demo._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Encoder
import org.apache.spark.rdd.RDD
import scalapb.spark.Implicits._
import scalapb.spark.ProtoSQL
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

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
    val rawEventDS = personsDS2.map({ row: Person => 
        RawEvent(
          metadata = "some useful wrapping metadata",
          value = row.toByteArray
        )
      })
    rawEventDS.printSchema

    // Use the dataset API and scalapb GenericMessage to parse the data
    // This works
    val parsedDS: Dataset[ParsedEvent[Person]] = ParsedEvent.fromRawEventDSasPerson(rawEventDS)
    parsedDS.show(false)
    // this fails to compile (see ParsedEvent.fromRawEventDS)
    val parsedDS2: Dataset[ParsedEvent[Person]] = ParsedEvent.fromRawEventDS[Person](rawEventDS)
    parsedDS2.show(false)
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

case class RawEvent (
  metadata: String,
  value: Array[Byte]
)

case class ParsedEvent[A] (
  metadata: String,
  value: A
)
object ParsedEvent {
  // inspired by https://scalapb.github.io/docs/generic/
  def fromRaw[A <: GeneratedMessage](
      raw: RawEvent
  )(implicit
      companion: GeneratedMessageCompanion[A]
  ): ParsedEvent[A] = {
    ParsedEvent(
      metadata = raw.metadata,
      value = companion.parseFrom(raw.value)
    )
  }

  // This succeeds, but is locked to Person
  def fromRawEventDSasPerson(ds: Dataset[RawEvent]): Dataset[ParsedEvent[Person]] = {
    ds.map(raw => ParsedEvent.fromRaw[Person](raw))
  }

  // This fails to compile 
  // RunDemo.scala:119:11: Unable to find encoder for type myexample.ParsedEvent[A]. An implicit Encoder[myexample.ParsedEvent[A]] is needed to store myexample.ParsedEvent[A] instances in a Dataset. Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases.
  // [error]     ds.map(raw => ParsedEvent.fromRaw[A](raw))
  // [error]           ^
  def fromRawEventDS[A <: GeneratedMessage : GeneratedMessageCompanion : Encoder](
    ds: Dataset[RawEvent]
  ): Dataset[ParsedEvent[A]] = {
    ds.map(raw => ParsedEvent.fromRaw[A](raw))
  }
}
