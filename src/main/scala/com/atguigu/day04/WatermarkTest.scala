package com.atguigu.day04

import org.apache.flink.runtime.state.Keyed
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream1: DataStream[String] = env.socketTextStream("hadoop105", 9999)
    val kvStream1: DataStream[(String, Long)] = stream1.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    }).assignAscendingTimestamps(_._2)

    val stream2: DataStream[String] = env.socketTextStream("hadoop105", 9998)
    val kvStream2: DataStream[(String, Long)] = stream2.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0), arr(1).toLong * 1000L)
    }).assignAscendingTimestamps(_._2)


    kvStream1.union(kvStream2).keyBy(_._1).process(new MyKeyed).print()

    env.execute()


  }

  class MyKeyed extends KeyedProcessFunction[String,(String,Long),String] {
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      collector.collect("当前水位线是:"+context.timerService().currentWatermark())
    }
  }

}
