package com.intelia.api.richFunction

import com.intelia.api.SensorReading
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 6:21
 * @version
 */
class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,String]{
  /*
  * @param <K> Type of the key.
  * @param <I> Type of the input elements.
  * @param <O> Type of the output elements.
  */
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String,SensorReading,String]#Context, out: Collector[String]): Unit = {

  }
}
