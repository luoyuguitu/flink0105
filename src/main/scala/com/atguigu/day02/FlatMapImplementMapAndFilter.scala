package com.atguigu.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapImplementMapAndFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //功能：把传感器id抽取出来
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    /*stream.flatMap(new FlatMapFunction[SensorReading,String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = out.collect(value.id)
    }).print()*/

    stream.flatMap(new FlatMapFunction[SensorReading,String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        if ("sensor_1".equals(value.id)){
          out.collect(value.id)
        }
      }
    }).print()

    env.execute()
  }

}
