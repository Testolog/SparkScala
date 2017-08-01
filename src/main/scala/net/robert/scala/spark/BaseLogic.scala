package net.robert.scala.spark

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class BaseLogic {
  def info(dataFrame: DataFrame): DataFrame = {
    val schema = dataFrame.schema.fieldNames
    val base = dataFrame.flatMap(x => {
      x.getValuesMap[String](schema).map(index => {
        Row(index._1, index._2)
      })
    }).filter(x => {
      if (x.getString(1) != null) {
        !x.getString(1).isEmpty && !x.getString(1).equals(" ")
      } else {
        true
      }
    })
    val structure = new StructType(Array(
      StructField("key", StringType, nullable = false),
      StructField("value", StringType, nullable = true)
    ))
    var changes = dataFrame.sqlContext.createDataFrame(base, structure)

    countByName(changes)
  }

  def countByName(dataFrame: DataFrame): DataFrame = {
    val collection_list = new CollectionList[ArrayType](ArrayType(StringType))
    val base = dataFrame.repartition(3)
      .groupBy(col("value"))
      .agg(count(col("value")).alias("value_count"))
      .repartition(3)
      .join(dataFrame.repartition(3)
        .groupBy(col("key"), col("value"))
        .agg(countDistinct("value").as("stg_count"))
        .repartition(3), "value")
    base.join(base.repartition(3)
      .groupBy(col("key"))
      .agg(sum("stg_count").as("unique_value")),
      "key")
      .repartition(3)
      .select(col("key"), col("unique_value"), array(col("value"), col("value_count")).as("values"))
      .groupBy(col("key"), col("unique_value"))
      .agg(collection_list(col("values")).alias("values"))
  }

  def validate(dataFrame: DataFrame, schema: String): DataFrame= {
    dataFrame.columns.foldLeft(dataFrame)((df, name) =>{
      dataFrame.withColumn(name)
    })
  }
}
