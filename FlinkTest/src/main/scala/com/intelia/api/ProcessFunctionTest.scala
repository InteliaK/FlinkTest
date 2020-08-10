package com.intelia.api

import com.intelia.api.richFunction.TempChangeWaring
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 6:17
 * @version
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("hadoop201",7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map{
        data =>
          val datas: Array[String] = data.split(",")
          SensorReading(datas(0),datas(1).toLong,datas(2).toDouble)
      }
//      .keyBy(_.id)
//      .process(new MyKeyedProcessFunction)
    val warningStream = dataStream
      .keyBy(_.id)
      .process(new TempChangeWaring(10000L))

    warningStream.print()


    env.execute("process_function_test")
  }
}
