package com.atguigu.day03

import com.atguigu.day02.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowTeampReduce {
  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    stream
      .map(r => (r.id, r.timepreture, r.timepreture))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        // 增量聚合函数
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        // 全窗口聚合函数
        new WindowResult
      ).print()

    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val minMax = elements.head
      out.collect(MinMaxTemp(key, minMax._2, minMax._3, context.window.getEnd))
    }
  }

}
