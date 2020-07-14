package com.atguigu.day03

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object avgTmpByAggregateFunction {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dstream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val keyDstream: KeyedStream[SensorReading, String] = dstream.keyBy(_.id)
    val winDstream: WindowedStream[SensorReading, String, TimeWindow] = keyDstream.timeWindow(Time.seconds(5))
    winDstream.aggregate(new AvgTempAgg).print()


    env.execute()
  }

  class AvgTempAgg extends AggregateFunction[SensorReading,(String,Long,Double),(String,Double)] {
    override def createAccumulator(): (String, Long, Double) = ("",0L,0.0)

    override def add(value: SensorReading, accumulator: (String, Long, Double)): (String, Long, Double) = {
      (value.id,accumulator._2+1L,accumulator._3+value.timepreture)
    }

    override def getResult(accumulator: (String, Long, Double)): (String, Double) = {
      (accumulator._1,accumulator._3/accumulator._2)
    }

    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (a._1,a._2+b._2,a._3+b._3)


    }
  }

}
