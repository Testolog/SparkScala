package net.robert.scala.spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.io.Source
import scala.util.parsing.json._

class TestBaseLogic extends FunSuite {
  val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[2]"))
  val context: SQLContext = new SQLContext(sc)
  val schema = new StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("age", StringType, nullable = true),
    StructField("birthday", StringType, nullable = true),
    StructField("gender", StringType, nullable = true)
  ))
  val rdd = Seq(
    Row("John", "26", "26-01-1995", "male"),
    Row("Lisa", "xyz", "26-01-1996", "female"),
    Row(null, "26", "26-01-1995", "male"),
    Row("Julia", " ", "26-01-1995", "female"),
    Row("", null, "26-01-1995", null),
    Row("Pete", "", "26-01-1995", "")
  )
  val testDF = context.createDataFrame(sc.parallelize(rdd), schema)
  val base = new BaseLogic()
  val source = JSON.parseFull(Source.fromURL(getClass.getResource("/excpect.json")).getLines.reduceLeft(_ + _))

  test("check info") {
    print(base.info(testDF.repartition(3)).show())
    //    print(source)
  }
}
