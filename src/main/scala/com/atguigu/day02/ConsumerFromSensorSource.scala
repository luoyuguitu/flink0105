package com.atguigu.day02

import org.apache.flink.streaming.api.datastream.DataStreamSource

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object ConsumerFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //调用addSource
    val stream: DataStreamSource[SensorReading] = env.addSource(new SensorSource)

    stream.print()

    env.execute()



  }

}
