package com.atguigu.day02

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.immutable
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading]{
  //表示数据源是否运行正常
  var running: Boolean = true


  //上下文参数来发送数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    //使用高斯噪声产生随机温度
    val curFtemp = (1 to 10).map(
      i => ("sensor_" + i, rand.nextGaussian() * 20)
    )

    //产生无限流数据
    while(running) {
      val mapTemp: immutable.IndexedSeq[(String, Double)] = curFtemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 10))
      )


      //产生时间戳
      val curTime: Long = Calendar.getInstance().getTimeInMillis

      //发送出去
      mapTemp.foreach( t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

      //每隔100ms发送一条传感器数据
      Thread.sleep(100)


    }












  }

  override def cancel(): Unit = running =false
}
