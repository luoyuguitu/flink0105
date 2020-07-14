package com.atguigu.day05

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1: KeyedStream[(String, String, Long), String] = env.fromElements(("1","click",3600*1000L),("1","click",3800*1000L)).assignAscendingTimestamps(_._3).keyBy(_._1)
    val stream2: KeyedStream[(String, String, Long), String] = env.fromElements(("1","browse",3300*1000L),("1","browse",3600*1000L),("1","browse",3000*1000L),("1","browse",2000*1000L)).assignAscendingTimestamps(_._3).keyBy(_._1)

    stream1.intervalJoin(stream2).between(Time.seconds(-600),Time.seconds(0)).process(new MyIntervalJoin).print()


    env.execute()








  }

  class MyIntervalJoin extends ProcessJoinFunction[(String, String, Long), (String, String, Long), String] {
    override def processElement(in1: (String, String, Long), in2: (String, String, Long), context: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, collector: Collector[String]): Unit = {
      collector.collect(in1 + "==============================>" + in2)
    }
  }

}
