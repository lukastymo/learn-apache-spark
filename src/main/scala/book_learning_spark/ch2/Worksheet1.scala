package book_learning_spark.ch2

import org.apache.spark.{SparkConf, SparkContext}

object Worksheet1 extends App {
  val conf = new SparkConf().setMaster("local").setAppName("My app")
  val sc = new SparkContext(conf)
  val input = sc.textFile("datasets/lorem.txt")
  val words = input.flatMap(_.split(" "))
  val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }

  counts.saveAsTextFile("target/output")
}
