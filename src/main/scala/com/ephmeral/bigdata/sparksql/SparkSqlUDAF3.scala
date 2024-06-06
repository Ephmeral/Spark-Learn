package com.ephmeral.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSqlUDAF3 {
  def main(args: Array[String]): Unit = {
    // 创建sparkSQL运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark SQL basic")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._
    // 早期版本中，spark不能在sql中使用强类型UDAF操作
    // SQL & DSL
    // 早期的UDAF强类型使用的是DSL

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    val ds: Dataset[User] = df.as[User]

    // 将UDAF函数转化为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show

    // 关闭环境
    spark.close();
  }

  /*
  * 自定义聚合函数类：计算年龄的平均值
  * 1. 继承自Aggregator
  *  IN: 输入的数据类型
  *  BUFF: 缓冲区数据类型
  *  OUT: 输出的数据类型
  * 2. 重写方法
   */
  case class User(username: String, age: Long)
  case class Buff(var total: Long, var count: Long);

  class MyAvgUDAF extends Aggregator[User, Buff, Long] {
    // 初始值或零值，缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: User): Buff = {
      buff.total = buff.total + in.age
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product[Buff]

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
