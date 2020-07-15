package com.atguigu.day06

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer

object FlinkWriteToKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[String] = env.fromElements("hello", "baby")

    stream.addSink(new FlinkKafkaProducer010[String](
      "hadoop108:9092,hadoop109:9092,hadoop110:9092",
      "flinkTest1",
      new SimpleStringSchema() //使用字符串格式写入kafka

    ))
    env.execute()

  }


}
