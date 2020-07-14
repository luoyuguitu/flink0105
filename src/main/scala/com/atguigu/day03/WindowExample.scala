package com.atguigu.day03

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dstream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val fnlDstream: DataStream[SensorReading] = dstream.keyBy(_.id).timeWindow(Time.seconds(10),Time.seconds(5)).reduce((s1,s2)=>SensorReading(s1.id,0L,s1.timepreture.min(s2.timepreture)))

    fnlDstream.print()

    env.execute()









  }

}
