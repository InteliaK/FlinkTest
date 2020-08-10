package com.intelia.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
/**
 * @description TODO 流处理WordCount
 * @author Intelia
 * @date 2020.8.4 14:05
 * @mogified By:
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //从外部命令中提取参数  作为socket主机名和端口号
        val paramTool : ParameterTool = ParameterTool.fromArgs(args)
        val host : String = paramTool.get("host")
        val port : Int = paramTool.getInt("port")

        //创建流处理的执行环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


        //接受一个socket文本流
        val inputDataStream: DataStream[String] = env.socketTextStream(host,port)
        //进行转换处理统计
        val resultDataStream = inputDataStream
          .flatMap(_.split(" "))
          .slotSharingGroup("a")
          .filter(_.nonEmpty)
          .slotSharingGroup("b")
          .map((_,1))
          .slotSharingGroup("a")
          .keyBy(0)
          .sum(1)

        resultDataStream.print()

        //启动任务执行，
        env.execute("Socket stream word count")

    }

}
