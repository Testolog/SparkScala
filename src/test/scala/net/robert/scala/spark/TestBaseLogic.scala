package net.robert.scala.spark

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.json4s.JsonDSL
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
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
  val new_schema =  JSON.parseFull(Source.fromURL(getClass.getResource("/new_schema.json")).getLines.reduceLeft(_ + _))
  val schemaList = new_schema.get.asInstanceOf[List[Map[String, String]]]
  test("check info") {
    var _df = base.validate(base.filterEmpty(testDF.repartition(3)), schemaList)
//    base.info(base.transform(_df)).select(explode(col("values"))).show()
    base.info(base.transform(_df)).show()
    /*
    * +---------------+
    |            col|
    +---------------+
    |[12/31/1995, 1]|
    |[01/01/1995, 2]|
    |      [John, 1]|
    |      [Lisa, 1]|
    |        [26, 2]|
    +---------------+
    ????
    *
*
*
* */
  }

  test("check transform") {
    base.transform(testDF.repartition(3)).show()
  }
  test("check validate") {
    assert(base.validate(testDF.repartition(3), schemaList).columns.sameElements(Array("first_name", "total_years", "d_o_b")))
  }
}
