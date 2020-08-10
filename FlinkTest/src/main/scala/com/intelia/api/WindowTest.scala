package com.intelia.api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.8 5:05
 * @version
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream = env.readTextFile("D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\out.txt")

    val inputStream = env.socketTextStream("hadoop201",7777)

    val dataStream = inputStream.map{
      data =>
        val datas = data.split(",")
        SensorReading(datas(0),datas(1).toLong,datas(2).toDouble)
    }
//      .assignAscendingTimestamps(_.timestamp * 1000L)//升序数据提取时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    val latetag = new OutputTag[(String,Double,Long)]("late")
    val resultDStream : DataStream[(String,Double,Long)]  = dataStream
      .map(data => (data.id,data.temperature,data.timestamp))
      .keyBy(_._1)
//        .window(TumblingEventTimeWindows.of(Time.seconds(5)))  //滚动时间窗口
//        .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(5)))  //滑动时间窗口
//        .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  //会话窗口
//        .countWindow(10)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
      .reduce{
          (curRes,newData) => (curRes._1,curRes._2.min(newData._2),newData._3)
      }


    resultDStream.getSideOutput(latetag).print("late")
    resultDStream.print("result")


    env.execute()
  }
}
