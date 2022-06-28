package myexample

import com.example.protos.demo._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import scalapb.spark.Implicits._

object RunStreamingDemo {

  def main(Args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ScalaPB Streaming Demo").getOrCreate()

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()
      .map(row => Person().update(
        _.name := row.getTimestamp(0).toString,
        _.age := row.getLong(1).toInt,
        _.gender := Gender.FEMALE)
      )

    val streamingQuery = rateStream.writeStream
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(1000))
      .foreachBatch(processMicroBatch _)
      .format("console")
      .start()

    streamingQuery.awaitTermination()

  }

  def processMicroBatch(batch: Dataset[Person], batchID: Long): Unit = {
    val result = batch.select("age", "name").collect()
    println(result)
  }

}
