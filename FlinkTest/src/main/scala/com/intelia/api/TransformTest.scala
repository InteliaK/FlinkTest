package com.intelia.api
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.5 15:39
 * @version
 */
object TransformTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
         env.setParallelism(1)

        //读取数据

        val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
        val inputStream = env.readTextFile(inpath)

        val dataStream = inputStream
          .map { data =>
              val arr = data.split(",")
              SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
          }

        //传出每个传感器的温度最小值
        val aggStream = dataStream
          .keyBy("id")
          .min("temperature")

        //aggStream.print()
        //输出最小的温度值，  附带最近的时间戳
        val resultStream = dataStream
            .keyBy("id")
//            .reduce(
//                (current,newData) =>
//                  SensorReading(current.id,newData.timestamp,current.temperature.min(newData.temperature))
//            )
            .reduce(new MyReduceFunction)
        //resultStream.print()


        //TODO 多流转换
        //分流
        val splitStream: SplitStream[SensorReading] = dataStream
          .split(data => {
              if (data.temperature > 30) Seq("high") else Seq("low")
          })
        val highTempStream: DataStream[SensorReading] = splitStream.select("high")
        val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
        val allTempStream: DataStream[SensorReading] = splitStream.select("low","high")

//        highTempStream.print()
//        lowTempStream.print()
//        allTempStream.print()

        //合流
        val warningSteam  = highTempStream.map(data => (data.id,data.temperature))
         val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningSteam.connect(lowTempStream)

        val coMapResultStream: DataStream[Product] = connectedStreams.map(
            warningData => (warningData._1, warningData._2, "warning"),
            lowTempData => (lowTempData.id, "healthy")
        )
//        coMapResultStream.print()

        //Union
        val unionStream: DataStream[SensorReading] = highTempStream.union(allTempStream,lowTempStream)
        unionStream.print("union_stream")


        env.execute()
    }
}
class MyReduceFunction extends ReduceFunction[SensorReading]{
    override def reduce(t1: SensorReading, t2: SensorReading): SensorReading = {
        SensorReading(t1.id,t2.timestamp,t1.temperature.min(t2.temperature))
    }
}