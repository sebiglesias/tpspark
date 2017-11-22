/**
  * Created by sebastian.iglesias on 21/11/2017.
  */

import org.apache.spark.SparkContext

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "InvertedIndexApp")
    val textFile = sc.wholeTextFiles("/Users/sebastian.iglesias/projects/faculty/Distribuidos/tpspark/src/main/scala/data/books/*")
    textFile.flatMap{
      case (name, content) =>
        val words = content.toLowerCase.split("""\W+""")
        words.map(word => (word,Set(name):Set[String]))
    }.reduceByKey{
      case (list1, list2) =>
        list1 ++ list2
    }.saveAsTextFile("/Users/sebastian.iglesias/projects/faculty/Distribuidos/tpspark/src/main/scala/output")
    sc.stop()
  }
}
