package com.atguigu.day04

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoprocessFunctionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //第一条流是无限流
    val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

    //第二条流是有限流，对第一条流做开关
    val switches: DataStream[(String, Long)] = env.fromElements(("sensor_2",10*1000L))

    val cntStream: ConnectedStreams[SensorReading, (String, Long)] = readings.connect(switches)

    cntStream.keyBy(fun1 =>fun1.id,_._1).process(new ReadingFilter).print()
    env.execute()






  }

  class ReadingFilter extends CoProcessFunction[SensorReading,(String,Long),SensorReading] {

    //初始值是false
    //每一个key都有对应的状态变量
    lazy val forwardingEnable: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean]))
    override def processElement1(in1: SensorReading, context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      //处理第一条流
      if(forwardingEnable.value()){
        collector.collect(in1)
      }



    }

    override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      //打开开关
      forwardingEnable.update(true)

      //开关的第二个值是放行时间
      val ts: Long = context.timerService().currentProcessingTime()+in2._2
      context.timerService().registerProcessingTimeTimer(ts)


    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      //关闭开关
      forwardingEnable.clear()
    }
  }

}
