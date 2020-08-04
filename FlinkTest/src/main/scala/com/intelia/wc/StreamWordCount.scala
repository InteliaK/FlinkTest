package com.intelia.wc

import org.apache.flink.streaming.api.scala._
/**
 * @description TODO 流处理WordCount
 * @auther Intelia
 * @date 2020.8.4 14:05
 * @mogified By:
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //创建流处理的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //接受一个socket文本流
        val inputDataStream: DataStream[String] = env.socketTextStream("hadoop201",7777)

        //进行转换处理统计
        val resultDataStream = inputDataStream
          .flatMap(_.split(" "))
          .filter(_.nonEmpty)
          .map((_,1))
          .keyBy(0)
          .sum(1)

        resultDataStream.print()

        //启动任务执行，
        env.execute("Socket stream word count")

    }
}
