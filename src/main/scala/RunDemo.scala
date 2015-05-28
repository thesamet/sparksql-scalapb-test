import com.example.protos.demo._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import com.trueaccord.scalapb.spark._

object RunDemo extends App {
    // Should be placed on all worker machines:
    val inputFile = "/tmp/input.base64.txt"
    val conf = new SparkConf().setAppName("ScalaPB Demo")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Converts a base64-encoded line to Person.
    def parseLine(s: String): Person =
      Person.parseFrom(
        org.apache.commons.codec.binary.Base64.decodeBase64(s))

    val persons: RDD[Person] = sc.textFile(inputFile).map(parseLine)

    // the above import com.trueaccord.scalapb.spark._ is needed for the following
    // to work:
    sqlContext.protoToDF(persons).registerTempTable("persons")

    sqlContext.sql("SELECT name, age, size(addresses) FROM persons WHERE age > 30")
      .collect
      .foreach(println)
}
