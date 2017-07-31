package net.robert.scala.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object App {


  def main(args: Array[String]) {
    var conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test-scala")
    var sc = new SparkContext(conf)
    var context = new SQLContext(sparkContext = sc)
    val path = ""
    val schema_path = ""
    val outPath = ""
    var df = context.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("")
    val basic = new BaseLogic()
    df = basic.validate(df, Source.fromFile(file = schema_path).getLines.reduceLeft(_ + _))
    df = basic.transfer(df)
    df = basic.info(df)
    df.write.json(outPath)
//    var df = context.createDataFrame(sc.parallelize(Seq(("a","1"),("b","2"))))
//    df.collect().foreach(print)
  }

}
