package net.robert.scala.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class BaseLogic {
  def transform(dataFrame: DataFrame): DataFrame = {
    val schema = dataFrame.schema.fieldNames
    val base = dataFrame
      .flatMap(x => {
        x.getValuesMap[String](schema)
          .map(index => {
            if (index._2 != null) {
              Row(index._1, String.valueOf(index._2))
            }
            else {
              Row(index._1, null)
            }
          })
      })
    val structure = new StructType(
      Array(
        StructField("key", StringType, nullable = true),
        StructField("value", StringType, nullable = true)
      ))
    dataFrame.sqlContext.createDataFrame(base, structure)
  }

  def filterEmpty(dataFrame: DataFrame): DataFrame = {
    dataFrame.sqlContext.createDataFrame(dataFrame.rdd.filter(x => {
      val rg = 0 until x.length
      val stg = rg.filter(elem => {
        if (x.getString(elem) != null) {
          !x.getString(elem).isEmpty && !x.getString(elem).equals(" ")
        } else {
          true
        }
      })
      stg.length == x.length
    }), dataFrame.schema)
  }

  def info(dataFrame: DataFrame): DataFrame = {
    val collection_list = new CollectionList[ArrayType](ArrayType(StringType))
    val base = dataFrame
      .repartition(3)
      .groupBy(col("value"))
      .agg(count(col("value")).alias("value_count"))
      .repartition(3)
      .join(dataFrame
        .repartition(3)
        .groupBy(col("key"), col("value"))
        .agg(countDistinct("value").as("stg_count"))
        .repartition(3),
        "value")
    base
      .join(base
        .repartition(3)
        .groupBy(col("key"))
        .agg(sum("stg_count").as("unique_value")),
        "key")
      .repartition(3)
      .select(col("key"),
        col("unique_value"),
        array(col("value"), col("value_count")).as("values"))
      .groupBy(col("key"), col("unique_value"))
      .agg(collection_list(col("values")).alias("values"))
  }

  def validate(dataFrame: DataFrame, schema: List[Map[String, String]]): DataFrame = {
    //without hardcode of type? from my point of view don't hardcode only one time, if type write correct.
    // In additional dynamic change type like
    //date or decimal witch have of second option like scale or format could not to realise  in test project.
    var cols = schema.map(x => {
      x.values.toList.head
    })
    var tmp = dataFrame.select(cols.head, cols.tail: _*)
    tmp.columns.foldLeft(tmp)((df, name) => {
      val _tmp = schema.filter(x => {
        x.get("existing_col_name").contains(name)
      })
      val newType = _tmp.head("new_data_type")
      var _df = df.select("*")
      if (newType.contains("date")) {
        //other way use of regex for declaring date format
        _df = _df.withColumn(name, from_unixtime(unix_timestamp(col(name), "DD-mm-YYYY"), _tmp.head("date_expression")))
      } else {
        _df = _df.withColumn(name, col(name).cast(newType))
      }
      _df.withColumnRenamed(name, _tmp.head("new_col_name"))
    })
  }
}
