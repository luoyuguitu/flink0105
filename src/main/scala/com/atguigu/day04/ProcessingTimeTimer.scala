package com.atguigu.day04

import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessingTimeTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .socketTextStream("hadoop105", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .keyBy(_._1)
      .process(new MyKeyed2)

    stream.print()

    env.execute()
  }


  class MyKeyed2 extends KeyedProcessFunction[String,(String,Long),String] {
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, collector: Collector[String]): Unit = {
      context.timerService().registerEventTimeTimer(context.timerService().currentProcessingTime() + 10* 1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("定时器触发了！"+"定时器的触发时间戳："+new Timestamp(timestamp))
    }
  }

}
