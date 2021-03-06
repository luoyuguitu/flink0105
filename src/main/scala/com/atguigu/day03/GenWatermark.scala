package com.atguigu.day03

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object GenWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 系统默认每隔200ms插入一次水位线
    // 设置为每隔一分钟插入一次水位线
    env.getConfig.setAutoWatermarkInterval(60000)

    val dstream: DataStream[String] = env.socketTextStream("hadoop105", 9999, '\n')
      .map(line => {
        //事件时间的单位必须是毫秒
        val arr: Array[String] = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      }
      )
      //分配时间戳和水位线一定要在keyBy之前进行
      //水位线 = 系统观察到的最大事件事件 - 最大延迟时间

      .assignTimestampsAndWatermarks(
        new MyAssigner

      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new CountByWindow)
    dstream.print()
    env.execute()


  }

  class MyAssigner extends AssignerWithPeriodicWatermarks[(String, Long)] {
    //设置最大的延迟时间
    val bound = 10 *1000L
    //系统观察到的元素包含的最大时间戳
    var maxTs = Long.MinValue +bound
    override def getCurrentWatermark: Watermark = {

      new Watermark(maxTs - bound)
    }

    override def extractTimestamp(t: (String, Long), l: Long): Long = {
      maxTs = maxTs.max(t._2)
      t._2

    }
  }

}

class CountByWindow extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    out.collect(new Timestamp(context.window.getStart) + "~~~~" + new Timestamp(context.window.getEnd) + "的窗口中有" + elements.size + "个元素！")
  }


}
