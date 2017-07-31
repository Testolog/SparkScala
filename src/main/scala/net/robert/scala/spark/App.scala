package net.robert.scala.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object App {


  def main(args: Array[String]) {
    var conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test-scala")
    var sc = new SparkContext(conf)
    var context = new SQLContext(sparkContext = sc)
//    var df = context.createDataFrame(sc.parallelize(Seq(("a","1"),("b","2"))))
//    df.collect().foreach(print)
  }

}
