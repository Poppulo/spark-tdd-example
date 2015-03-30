package com.newsweaver.sparktddexample

import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by kduggan on 27/03/2015.
 */
object WordCountExample {

  def runApp(sc: SparkContext): Unit ={
    val file = sc.textFile("sampleWords.txt")
    countWordsInFile(file).saveAsTextFile("results.txt")
  }

  def countWordsInFile = splitFile _ andThen countWords _

  def splitFile(wordsByLine: RDD[String]): RDD[String] = {
    wordsByLine.flatMap(line => line.split(" "))
  }

  def countWords(words: RDD[String]): RDD[(String, Int)] = {
    words.map(word => (word, 1)).reduceByKey(_ + _)
  }

}
