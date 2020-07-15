package com.atguigu.day06

import java.util.Properties

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object WriteToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop108").build()
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream.addSink(new RedisSink[SensorReading](conf, new MyRedis))
    env.execute()

  }

  case class MyRedis() extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor")
    }

    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = t.timepreture.toString
  }

}
