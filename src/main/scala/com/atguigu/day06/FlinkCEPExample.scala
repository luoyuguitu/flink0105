package com.atguigu.day06


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkCEPExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements(
      LoginEvent("user_1", "fail", "0.0.0.1", 1000L),
      LoginEvent("user_1", "fail", "0.0.0.2", 2000L),
      LoginEvent("user_1", "fail", "0.0.0.3", 3000L),
      LoginEvent("user_2", "success", "0.0.0.1", 4000L)
    )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    val pattern = Pattern.begin[LoginEvent]("first")
      .where(x=>"fail".equals(x.eventType))
        .next("second")
        .where(x=>"fail".equals(x.eventType))
      .next("third")
      .where(x=>"fail".equals(x.eventType))
        .within(Time.seconds(10))
    //第一个参数是待匹配的流，第二个参数是匹配规则
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(stream,pattern)

    //处理匹配到的事件
    patternStream.select((pattern:scala.collection.Map[String,Iterable[LoginEvent]]) => {
      val first = pattern("first").iterator.next()
      val second = pattern("second").iterator.next()
      val third = pattern("third").iterator.next()

      "用户 " + first.userId + " 分别在ip: " + first.ipAddr + " ; " + second.ipAddr + " ; " + third.ipAddr + " 登录失败！"

    }).print()

    env.execute()

  }

  case class LoginEvent(userId: String, eventType: String, ipAddr: String, eventTime: Long)

}
