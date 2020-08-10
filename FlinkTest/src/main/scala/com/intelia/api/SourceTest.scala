package com.intelia.api

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable
import scala.util.Random

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.5 11:52
 * @version
 */
object SourceTest {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        //TODO 1. 从集合中获取
//        val stream1 = env
//          .fromCollection(List(
//              SensorReading("sensor_1", 1547718199, 35.8),
//              SensorReading("sensor_6", 1547718201, 15.4),
//              SensorReading("sensor_7", 1547718202, 6.7),
//              SensorReading("sensor_10", 1547718205, 38.1)
//          ))
//
//        stream1.print("Stream1").setParallelism(1)
//
//        //TODO 2. 从文件中读取
//        val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
//        val stream2 = env.readTextFile(inpath)
//        stream2.print("Stream2").setParallelism(1)
//
//
//        //TODO 3. 从kafka读取数据
//        import java.util.Properties
//        val properties = new Properties()
//        properties.setProperty("bootstrap.servers","hadoop201:9092")
//        properties.setProperty("group_id","consumer-group")
//
//        val stream3 = env.addSource(
//            new FlinkKafkaConsumer011[String](
//            "sensor",
//            new SimpleStringSchema(),
//            properties
//            )
//        )

        //TODO 4. 自定义Source
        val stream4 = env.addSource(new MySensorSource())

        stream4.print()


        env.execute()
    }
}


case class  SensorReading(
                           id: String,
                           timestamp: Long,
                           temperature: Double)

class  MySensorSource() extends SourceFunction[SensorReading]{

    //标志数据源状态
    var running = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        //定义随机数发生器
        val rand  = new Random()

        //生成十个随机数，作为初始温度
        var curTemp: immutable.IndexedSeq[(String, Double)] =
            1.to(10).map( i => ("sensor_" + i , rand.nextDouble() * 100))

        while(running){
            curTemp = curTemp.map(
                data => (data._1,data._2 + rand.nextGaussian())
            )

            val curTime = System.currentTimeMillis()
            curTemp.foreach(
                data => sourceContext.collect(SensorReading(data._1,curTime,data._2))
            )

            Thread.sleep(1000)
        }



    }

    override def cancel(): Unit = { running = false}

}
