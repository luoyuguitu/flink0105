package com.atguigu.day05

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //每10s做一次检查点操作
    env.enableCheckpointing(10000L)
    //配置检查点的路径
    //file+绝对路径
    env.setStateBackend(new FsStateBackend("file:\\D:\\MyWork\\WorkSpace\\Idea\\flink0105\\src\\main\\resources"))

    env.addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new MyKeyed3)
      .print()


    env.execute()


  }

  class MyKeyed3 extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
    )

    lazy val timer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //将元素添加到list
      listState.add(value)
      //如果是第一个元素，添加定时器
      if (timer.value() == 0L) {
        //注册一个10s后的定时器
        val ts: Long = ctx.timerService().currentProcessingTime() + 10 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        timer.update(ts)
      }


    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //利用回调函数访问后台状态
      // 不能直接对列表状态变量进行计数
      val readings: ListBuffer[SensorReading] = new ListBuffer()
      // 隐式类型转换必须导入
      import scala.collection.JavaConversions._
      for (r <- listState.get) {
        readings += r
      }
      out.collect("当前时刻列表状态变量里面共有 " + readings.size + "条数据！")
      timer.clear()

    }
  }

}
