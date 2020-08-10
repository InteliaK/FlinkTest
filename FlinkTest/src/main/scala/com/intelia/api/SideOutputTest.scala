package com.intelia.api

import com.intelia.api.richFunction.SplitTempProcessor
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 7:12
 * @version
 */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend(new RocksDBStateBackend(""))
    val inputStream = env.socketTextStream("hadoop201", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map {
        data =>
          val datas: Array[String] = data.split(",")
          SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    val highTempStream  = dataStream
      .process(new SplitTempProcessor(30.0))
    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String,Long,Double)]("low")).print("low")


    env.execute("sideOutput")
  }
}
