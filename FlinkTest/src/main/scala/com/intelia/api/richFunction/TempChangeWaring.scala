package com.intelia.api.richFunction

import com.intelia.api.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 6:
 * @version
 */
class TempChangeWaring(interval : Long) extends KeyedProcessFunction[String,SensorReading,String]{
  lazy val lastTempState : ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("last-temp",classOf[Double])
  )
  lazy val timeTsState : ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("time-ts",classOf[Long])
  )

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "的温度" + interval / 1000L + "s连续上升")
    timeTsState.clear()
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    val lastTemp = lastTempState.value()
    val timeTs = timeTsState.value()


    //更新温度值
    lastTempState.update(value.temperature)

    if(value.temperature > lastTemp && timeTs == 0L){
    //条件判定   温度上升&&定时器为空
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      timeTsState.update(ts)
    }else if(value.temperature < lastTemp){
      ctx.timerService().deleteProcessingTimeTimer(timeTs)
      timeTsState.clear()
    }
  }
}
