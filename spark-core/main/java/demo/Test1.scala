package demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    var sc = new SparkContext(sparkConf)
    val lines:RDD[String] = sc.textFile("datas/*")
    val words:RDD[String] = lines.flatMap(_.split(" "))

    // 统计每个单词出现的次数
    val wordToOne: RDD[(String, Int)] = words.map {
      word => (word, 1)
    }

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }
    val tuples: Array[(String, Int)] = wordCount.collect()
    tuples.foreach(println)
    sc.stop()
  }
}
