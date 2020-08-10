package com.intelia.api.sink

import com.intelia.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 21:44
 * @version
 */
object RedisTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
    val inputStream: DataStream[String] = env.readTextFile(inpath)

    val dataDStream = inputStream
      .map{
        data =>
          val datas: Array[String] = data.split(",")
          SensorReading(datas(0),datas(1).toLong,datas(2).toDouble)
      }

    val conf = new FlinkJedisPoolConfig.Builder()
        .setHost("hadoop201")
        .setPort(6379)
        .build()


    dataDStream.addSink(
      new RedisSink[SensorReading](conf,new MyRedisMapper)
    )

    env.execute("RedisTest")
  }

}
class MyRedisMapper extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}
