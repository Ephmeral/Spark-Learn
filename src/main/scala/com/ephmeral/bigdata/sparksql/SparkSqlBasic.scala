package com.ephmeral.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlBasic {
  def main(args: Array[String]): Unit = {
    // 创建sparkSQL运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark SQL basic")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import spark.implicits._

    // 执行逻辑操作
    // DataFrame
    //    val df: DataFrame = spark.read.json("data/user.json")
    // df.show()

    // DataFrame => SQL
    //    df.createOrReplaceTempView("user")
    //    spark.sql("select * from user").show
    //    spark.sql("select age, username from user where age > 20").show
    //    spark.sql("select avg(age) from user").show

    // DataFrame => DSL
    // 使用DataFrame时，涉及到转换操作，需要引入转换规则
    //    df.select("age", "username").show()
    //    df.select($"age" + 1).show
    //    df.select('age + 2).show

    // DataSet
    // DataFrame是特定泛型的DataSet
    //    val seq = Seq(1, 2, 3, 4, 5, 6, 7)
    //    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()

    // RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "wxl", 29), (2, "silas", 24)))
    val df: DataFrame = rdd.toDF()
    val rowRDD = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    // RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {}
        User(id, name, age)
    }.toDS()

    val userRDD: RDD[User] = ds1.rdd

    // 关闭环境
    spark.close();
  }

  case class User(id: Int, name: String, age: Int)
}
