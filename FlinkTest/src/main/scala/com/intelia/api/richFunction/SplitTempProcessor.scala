package com.intelia.api.richFunction

import com.intelia.api.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 7:16
 * @version
 */
class SplitTempProcessor(threshold : Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if(value.temperature > threshold){
      //温度值大于标准值 ，输出到主流
      out.collect(value)
    }else{
      //否则，输出到侧输出流
      ctx.output(new OutputTag[(String,Long,Double)]("low"),(value.id,value.timestamp,value.temperature))
    }
  }
}
