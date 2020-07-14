package com.atguigu.day02

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1: DataStream[(String, Int)] = env.fromElements(("wuyong",135),("yxm",100))
    val stream2: DataStream[(String, Int)] = env.fromElements(("wuyong",26),("yxm",24))

    //按照key将两条流联合在一起

    val connected: ConnectedStreams[(String, Int), (String, Int)] = stream1.keyBy(_._1).connect(stream2.keyBy(_._1))

    connected.map(new MyCoMapFunction).print()







    env.execute()
  }

  class MyCoMapFunction extends CoMapFunction[(String, Int), (String, Int),String] {
    override def map1(in1: (String, Int)): String = {
      in1._1 + "的体重是：" + in1._2 + "斤"
    }

    override def map2(in2: (String, Int)): String = {
      in2._1 + "的年龄是：" + in2._2 + "岁"
    }
  }

}
