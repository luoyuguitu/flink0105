package com.atguigu.day06

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FlinkReadFromKafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.put("bootstrap.servers","hadoop108:9092,hadoop109:9092,hadoop110:9092")
    props.put("group.id", "consumer-group")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")


    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011(
      "flinkTest1",
      new SimpleStringSchema(),
      props
    ))
    stream.print()

    env.execute()


  }

}
