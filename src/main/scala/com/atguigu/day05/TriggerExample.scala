package com.atguigu.day05

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[String] = env.socketTextStream("hadooop105", 9999)
    stream.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0), arr(1).toLong)
    })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTriger)
      .process(new WindowCount)
      .print()
    env.execute()


  }

  class OneSecondIntervalTriger extends Trigger[(String, Long), TimeWindow] {
    //每来一条数据调用一次此方法
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      //默认值是false
      //第一条数据来的时候，后边设置成true
      val firstSeen: ValueState[Boolean] = triggerContext.getPartitionedState(new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean]))
      //当第一条数据来的时候，firstSeen.value()为true
      //近对第一条数据注册
      if (!firstSeen.value()) {
        // 如果当前水位线是1234ms，那么t = 1234 + (1000 - 1234 % 1000) = 2000
        println("第一条数据来了！当前水位线是：" + triggerContext.getCurrentWatermark)
        val t = triggerContext.getCurrentWatermark + (1000 - (triggerContext.getCurrentWatermark % 1000))
        println("第一条数据来了以后，注册的定时器的整数秒的时间戳是：" + t)
        triggerContext.registerEventTimeTimer(t) // 在第一条数据的时间戳之后的整数秒注册一个定时器
        triggerContext.registerEventTimeTimer(w.getEnd) // 在窗口结束事件注册一个定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE


    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      // 在onElement函数中，我们注册过窗口结束时间的定时器
      if (l == w.getEnd) {
        // 在窗口闭合时，触发计算并清空窗口
        TriggerResult.FIRE_AND_PURGE
      } else {

        val t = triggerContext.getCurrentWatermark + (1000 - (triggerContext.getCurrentWatermark % 1000))
        if (t < w.getEnd) {
          println("注册的定时器的整数秒的时间戳是：" + t)
          triggerContext.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE
      }

    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
      // 状态变量是一个单例！
      val firstSeen = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      firstSeen.clear()
    }
  }


  case class WindowCount() extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect("窗口中有" + elements.size + "条数据！窗口的结束时间是：" + context.window.getEnd)
    }
  }


}
