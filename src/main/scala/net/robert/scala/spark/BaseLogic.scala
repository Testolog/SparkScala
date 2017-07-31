package net.robert.scala.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

class BaseLogic {

  def info(dataFrame: DataFrame): DataFrame = {
//    var base = dataFrame.columns.foldLeft(dataFrame)((acc, name) => {
//      countByName(acc, name)
//    })
//
//    base.show()
//
//    dataFrame
    countByName(dataFrame, "name")
  }

  def countByName(dataFrame: DataFrame, name: String): DataFrame = {
    val collection_list = new CollectionList[ArrayType](ArrayType(StringType))
    val base = dataFrame.repartition(3)
      .select(lit(name).alias("key"), col(name).alias("value"))
      .groupBy(col("value"))
      .agg(countDistinct(col("value")).alias("unique_value")).repartition(3)
      .select(array(col("value"), col("unique_value")).alias("values"), lit(name).alias("key"))
    val uniqueCount = dataFrame.repartition(3).select(lit(name).alias("key"), countDistinct(col(name)).alias("unique_count"))
    base.join(uniqueCount).repartition(3)
      .select(base.apply("key"), col("values"), col("unique_count"))
      .repartition(3)
      .groupBy("key", "unique_count")
      .agg(collection_list(col("values")).alias("values"))
      .select(col("key").alias("column"),
        col("unique_count").alias("unique_values"),
        col("values").alias("values")
      )


    //      .select(
    //        lit(name).alias("column"),
    //        countDistinct(col(name)).alias("uniqueValue")
    //      )
  }

  def transfer(dataFrame: DataFrame): DataFrame = {
    dataFrame.na.drop()
  }
  def validate(dataFrame: DataFrame, schema: String): DataFrame= {
    dataFrame
  }
}
