package com.atguigu.day02

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    //stream.map(_.id).print()

    stream.map(new MapFunction[SensorReading,String] {
      override def map(value: SensorReading): String = value.id
    }).print()

    stream.map(new MyMapFunction)

    env.execute()
  }

  class MyMapFunction extends MapFunction[SensorReading,String] {
    override def map(value: SensorReading): String = value.id
  }

}
