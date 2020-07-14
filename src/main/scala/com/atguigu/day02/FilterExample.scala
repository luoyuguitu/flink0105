package com.atguigu.day02

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

import scala.tools.nsc.io.SourceReader

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.filter(r =>"sensor_8".equals(r.id)).print()

    stream.filter(new MyFilterFunction)

    stream.filter(new FilterFunction[SensorReading] {
      override def filter(value: SensorReading): Boolean =  "sensor_8".equals(value.id)

    })





    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean =  "sensor_8".equals(value.id)

  }

}
