package com.atguigu.day06

import java.util

import com.atguigu.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object WriteToES {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop108",9200))

    val esSinkUtil = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val hashMap = new util.HashMap[String,String]()
          hashMap.put("data",t.toString)

          val indexRequest: IndexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(hashMap)

          requestIndexer.add(indexRequest)

        }
      }

    )
    esSinkUtil.setBulkFlushMaxActions(1)

    env.addSource(new SensorSource).addSink(esSinkUtil.build())


    env.execute()



  }



}
