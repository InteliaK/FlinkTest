package com.intelia.api.sink

import com.intelia.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 9:42
 * @version
 */
object FileTest {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
        val inputStream: DataStream[String] = env.readTextFile(inpath)

        val dataStream = inputStream
            .map{
                data =>
                    val datas: Array[String] = data.split(",")
                    SensorReading(datas(0),datas(1).toLong,datas(2).toDouble)
            }
        dataStream.addSink(
            StreamingFileSink.forRowFormat(
                new Path("D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\out1.txt"),
                new SimpleStringEncoder[SensorReading]()
            ).build()
        )

        env.execute("file sink")
    }

}
