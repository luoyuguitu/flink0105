package com.atguigu.day02

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource).filter(s=>"sensor_1".equals(s.id))


    val keyed: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val min: DataStream[SensorReading] = keyed.min(2)

    min.print()

    keyed.reduce((r1,r2) =>SensorReading(r1.id,0L,r1.timepreture.min(r2.timepreture))).print()









    env.execute()

  }

}
