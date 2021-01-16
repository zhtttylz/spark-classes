package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
     var sc = new SparkContext(sparkConf)
    val lines:RDD[String] = sc.textFile("datas/*")
    val words:RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word=>word)

    val wordCount: RDD[(String        , Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val tuples: Array[(String, Int)] = wordCount.collect()
    tuples.foreach(println)
    sc.stop()
  }
}
