package com.intelia.wc

import org.apache.flink.api.scala._


/**
 * @description TODO
 * @auther Intelia
 * @date 2020.8.4 12:03
 * @mogified By:
 */
object WordCount{
    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\helllo.txt"

        val inputDS: DataSet[String] = env.readTextFile(inpath)

        val resultDataSet: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))
          .map((_, 1))
          .groupBy(0)
          .sum(1)

        resultDataSet.print()
    }
}
