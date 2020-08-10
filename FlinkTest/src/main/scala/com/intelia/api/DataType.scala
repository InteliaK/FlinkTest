package com.intelia.api

import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 7:52
 * @version
 */
object DataType {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //TODO 基础数据类型
        val data1: DataStream[Long] = env.fromElements(1L,2L,3L,4L)
        //data1.print()

        //TODO 元组
        val data2: DataStream[(String, Int)] = env.fromElements(
            ("intelia", 28),
            ("diana", 19)
        )
        //data2.print()

        //样例类
        val data3 = env.fromElements(
            Person("intelia",20),
            Person("diana", age = 13)
        )
        data3.filter(_.age > 16).print()






        env.execute()
    }
}
case class Person(
                   name : String,
                   age : Int
                 )