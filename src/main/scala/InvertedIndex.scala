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
        words.map(word => (word,List(name):List[String]))
    }.reduceByKey{
      case (list1, list2) =>
        var result:List[String] = list1
        list2.foreach(filename =>
        if(!list1.contains(filename)) result = List(filename):::result)
        result
    }.saveAsTextFile("/Users/sebastian.iglesias/projects/faculty/Distribuidos/tpspark/src/main/scala/output")
    sc.stop()
  }
}
