package com.atguigu.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FaltMapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.fromElements("apple","mellon","banana")

    stream.flatMap(new MyFlatMapFunction).print()

    env.execute()

  }

  class MyFlatMapFunction extends FlatMapFunction[String,String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if("apple".equals(value)) {
        out.collect(value)
        out.collect(value)
      } else if ("banana".equals(value)) {
        out.collect(value)
      }


    }

  }

}
