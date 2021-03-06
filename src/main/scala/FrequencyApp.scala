/**
  * Created by sebastian.iglesias on 21/11/2017.
  */

import org.apache.spark.SparkContext

object FrequencyApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "FrequencyApp")
    val textFile = sc.textFile("/Users/sebastian.iglesias/projects/faculty/Distribuidos/tpspark/src/main/scala/data/books/*")
    val counts = textFile.flatMap(line => line.split("""\W+"""))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)
      .map(touple => (touple._2,touple._1))
      .top(50)
      .foreach(println(_))
  }
}
