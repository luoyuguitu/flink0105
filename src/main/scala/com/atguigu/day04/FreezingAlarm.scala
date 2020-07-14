package com.atguigu.day04

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FreezingAlarm {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource).process(new FreezingAlarmFunction)
    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print()//打印侧输出流

    env.execute()

  }

  //processFunction定义的是没有keyby的流
  class FreezingAlarmFunction extends ProcessFunction[SensorReading,SensorReading] {
    //定义一个测输出的标签，就是测输出流的名字
    lazy val freezingAlarmOut = new OutputTag[String]("freezing-alarm")
    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {

      if (i.timepreture <32.0) {

        context.output(freezingAlarmOut,i.id+"的传感器低温报警!")
      }
      //将所有数据发送到常规输出
      collector.collect(i)
    }


  }

}
