package com.intelia.api.richFunction

import com.intelia.api.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.util.Collector

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 5:18
 * @version
 */
class TempChangeAlert(threshold : Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //定义状态  保存温度值
  lazy val lastTempState = getRuntimeContext.getState[Double](
    new ValueStateDescriptor[Double]("last-temp",classOf[Double])
  )

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()

    val diff = (in.temperature - lastTemp).abs //求绝对值
    if(diff > threshold) collector.collect((in.id,lastTemp,in.temperature))

    //状态更新
    lastTempState.update(in.temperature)
  }
}
