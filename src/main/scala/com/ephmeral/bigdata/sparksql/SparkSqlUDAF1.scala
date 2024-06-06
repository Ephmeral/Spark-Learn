package com.ephmeral.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSqlUDAF1 {
  def main(args: Array[String]): Unit = {
    // 创建sparkSQL运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark SQL basic")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg", new MyAvgUDAF)
    spark.sql("select ageAvg(age) from user").show

    // 关闭环境
    spark.close();
  }

  /*
  * 自定义聚合函数类：计算年龄的平均值
  * 1. 继承自UserDefinedAggregateFunction
  * 2. 重写方法
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据的结构，Buffer
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 函数计算结果的数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L);
      buffer.update(1, 0L);
    }

    // 根据输入的值来更新缓冲区的值
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0));
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}
