package com.yiban.datacenter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.tools.nsc.transform.Flatten
import org.apache.spark.rdd.RDD

/**
 * @author Administrator
 */
class WordCount {
  
}
object WordCount{
  def main(args: Array[String]): Unit = {
  val conf = new SparkConf().setAppName("HelloWorld").setMaster("local")
  val context: SparkContext = new SparkContext(conf)
  val textFile : RDD[String] = context.textFile("hdfs://192.168.27.233:8020/user/liujiyu/input", 1)
  val tokenized = textFile.flatMap(_.split(" "))
  val wordCounts = tokenized.map((_, 1))
  val count = wordCounts.reduceByKey(_ + _)
  println(count.collect.mkString(", "));
  context.stop  
  }
}