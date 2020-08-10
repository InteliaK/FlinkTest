package com.intelia.api.sink

import com.intelia.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 20:35
 * @version
 */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
    val inputDStream: DataStream[String] = env.readTextFile(inpath)

    val dataDStream: DataStream[SensorReading] = inputDStream.map {
      data =>
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    }

    import java.util
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop201",9200))
    httpHosts.add(new HttpHost("hadoop202",9200))
    httpHosts.add(new HttpHost("hadoop203",9200))

    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //数据源
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("id",t.id)
        dataSource.put("temperature",t.temperature.toString)
        dataSource.put("timestamp",t.timestamp.toString)

        //发送http请求
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        requestIndexer.add(indexRequest)
      }


    }
    dataDStream.addSink(
      new ElasticsearchSink
        .Builder[SensorReading](httpHosts,myEsSinkFunc)
        .build()
    )

    env.execute()
  }
}
