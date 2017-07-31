package net.robert.scala.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class CollectionList[T](val colType: DataType) extends UserDefinedAggregateFunction {
  def inputSchema: StructType =
    new StructType().add("inputCol", colType)

  def bufferSchema: StructType =
    new StructType().add("outputCol", ArrayType(colType))

  def dataType: DataType = ArrayType(colType)

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, new scala.collection.mutable.ArrayBuffer[T])
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val list = buffer.getSeq[T](0)
    if (!input.isNullAt(0)) {
      val sales = input.getAs[T](0)
      buffer.update(0, list :+ sales)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getSeq[T](0).toList ++ buffer2.getSeq[T](0).toList)
  }

  def evaluate(buffer: Row): Any = {
    buffer.getSeq[T](0).toList
  }
}
