package com.atguigu.day05

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream1: DataStream[(String, Int, Long)] = env.fromElements(
      ("a", 1, 1000L),
      ("a", 2, 2000L),
      ("b", 1, 3000L),
      ("b", 2, 4000L))
      .assignAscendingTimestamps(_._3)

    val stream2 = env
      .fromElements(
        ("a", 10, 1000L),
        ("a", 20, 2000L),
        ("b", 10, 3000L),
        ("b", 20, 4000L)
      )
      .assignAscendingTimestamps(_._3)
    stream1.join(stream2)
      .where(_._1)
      .equalTo(_._1)
      //.window(new TumblingEventTimeWindows(Time.seconds(10)))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new MyJoinFunc)
      .print()
    env.execute()


  }

  class MyJoinFunc extends JoinFunction[(String, Int, Long), (String, Int, Long), String] {
    override def join(first: (String, Int, Long), second: (String, Int, Long)): String = {
      first + "========================>" + second
    }
  }


}
