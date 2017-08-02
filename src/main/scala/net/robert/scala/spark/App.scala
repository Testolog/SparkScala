package net.robert.scala.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.parsing.json.JSON

object App {

  def main(args: Array[String]) {
    var conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test-scala")
    var sc = new SparkContext(conf)
    var context = new SQLContext(sparkContext = sc)
    val path = args(0)
    val schema = args(1)
    val outPath = args(2)
    var df = context.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(path)
    val base = new BaseLogic()
    val new_schema =  JSON.parseFull(Source.fromFile(schema).getLines.reduceLeft(_ + _))
    val schemaList = new_schema.get.asInstanceOf[List[Map[String, String]]]
    var _df = base.filterEmpty(df.repartition(3))
    _df = base.validate(_df, schemaList)
    _df = base.transform(_df)
    base.info(_df).repartition(1).write.json(outPath)
//    var df = context.createDataFrame(sc.parallelize(Seq(("a","1"),("b","2"))))
    //    df.collect().foreach(print)
  }

}
