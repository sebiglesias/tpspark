/**
  * Created by sebastian.iglesias on 21/11/2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "InvertedIndexApp")
    val textFile = sc.wholeTextFiles("/Users/sebastian.iglesias/projects/faculty/Distribuidos/tpspark/src/main/scala/data/books/*")
  }
}
