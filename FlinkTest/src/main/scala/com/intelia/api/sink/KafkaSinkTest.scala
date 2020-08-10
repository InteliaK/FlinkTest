package com.intelia.api.sink

import java.util.Properties

import com.intelia.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 21:12
 * @version
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop201:9092")
    properties.setProperty("group.id","consumer_group")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))


    val dataDStream: DataStream[String] = stream.map(
      data => {
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble).toString
      }
    )

    dataDStream.addSink(new FlinkKafkaProducer011[String]("hadoop201:9092","test",new SimpleStringSchema()))

    env.execute("KafkaSink")
  }
}
