/**
  * Created by sebastian.iglesias on 21/11/2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FrequencyApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "FrequencyApp")
    val textFile = sc.textFile("hdfs://...")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://...")
  }
}
