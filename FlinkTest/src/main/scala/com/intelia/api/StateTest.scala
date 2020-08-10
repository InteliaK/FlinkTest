package com.intelia.api

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 4:30
 * @version
 */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //开启checkpoint
    env.enableCheckpointing()

    //checkpoint的配置
    val config: CheckpointConfig = env.getCheckpointConfig
    config.setCheckpointInterval(10000L)//checkpoint的时间间隔,向jobManager发送的时间间隔
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//设置存储级别
    config.setCheckpointTimeout(10000)//超时时间，丢弃checkpoint
    config.setMaxConcurrentCheckpoints(1)//设置流中最多有几个checkpoint
    config.setMinPauseBetweenCheckpoints(500L)//checkpoint结束，距下一次开始最短时间

    config.setPreferCheckpointForRecovery(true)//是否优先采用savepoint恢复

    config.setTolerableCheckpointFailureNumber(0)//允许checkpoint失败

    //重启策略
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(3,1000L) //(a,b)间隔b时间长度重启a次
    )
    env.setRestartStrategy(
      /*
       * a,b,c
       * 在b的时间限制启动a次，每次间隔c
       */
      RestartStrategies.failureRateRestart(5,Time.minutes(2),Time.seconds(10))
    )



    val inputStream = env.socketTextStream("hadoop201",7777)

    val dataStream: DataStream[SensorReading] = inputStream.map {
      data =>
        val datas: Array[String] = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    }
    //温度变化10度报警
    val resultStream: DataStream[(String, Double, Double)] = dataStream
      .keyBy(_.id)
//      .flatMap(new TempChangeAlert(10.0))
      .flatMapWithState[(String,Double,Double),Double]({//[待处理数据，状态值类型]  在keyStream才能调用
        case (data: SensorReading,None) =>(List.empty,Some(data.temperature))
        case (data: SensorReading ,lastTemp : Some[Double]) => {
          val diff = (data.temperature - lastTemp.get).abs //求绝对值
          if(diff > 10.0){
            (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
          }else{
            (List.empty,Some(data.temperature))
          }
        }
      })

    resultStream.print()





    env.execute()
  }
}
