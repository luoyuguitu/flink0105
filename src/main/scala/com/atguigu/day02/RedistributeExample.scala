package com.atguigu.day02

import com.atguigu.day02.RichFunctionExample.MyRichMap
import org.apache.flink.streaming.api.scala._

object RedistributeExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.addSource(new SensorSource).map(_.id).setParallelism(2).print()


    env.execute()




  }

}
