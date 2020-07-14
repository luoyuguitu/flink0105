package com.atguigu.day03

import com.atguigu.day02.{SensorReading, SensorSource}
import com.atguigu.day03.avgTmpByAggregateFunction.AvgTempAgg
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object avgTmpByProcessFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dstream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val keyDstream: KeyedStream[SensorReading, String] = dstream.keyBy(_.id)
    val winDstream: WindowedStream[SensorReading, String, TimeWindow] = keyDstream.timeWindow(Time.seconds(5))
    winDstream.process(new AvgTempProcess).print()


    env.execute()
  }

  class AvgTempProcess extends ProcessWindowFunction[SensorReading,AvgInfo,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgInfo]): Unit = {
      val count: Int = elements.size
      var sum =0.0
      for (elem <- elements) {
        sum =sum+elem.timepreture
      }

      val windowStart: Long = context.window.getStart
      val windowEnd: Long = context.window.getEnd
      out.collect(AvgInfo(key,sum/count,windowStart,windowEnd))



    }
  }

}

case class AvgInfo(id:String,avgTemp:Double,windowStart:Long,windowEnd: Long)
