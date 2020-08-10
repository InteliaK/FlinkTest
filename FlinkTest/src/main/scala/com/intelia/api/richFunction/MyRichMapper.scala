package com.intelia.api.richFunction

import java.util

import com.intelia.api.{MyReduceFunction, SensorReading}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.10 5:15
 * @version
 */
class MyRichMapper extends RichMapFunction[SensorReading,String]{
  var valueState: ValueState[Double] = _
  lazy val listState : ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("ListState",classOf[Int])
  )
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("MapState", classOf[String], classOf[Double])
  )
  lazy val reduceState : ReducingState[SensorReading] = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[SensorReading] ("reduceState",new MyReduceFunction,classOf[SensorReading])
  )


  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("ValueState",classOf[Double])
    )
  }




  override def map(in: SensorReading): String = {
    val myV: Double = valueState.value()
    valueState.update(in.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)//添加列表
    listState.update(list)//更新值
    listState.get()//获取值列表

    mapState.contains("sensor_1")//key对应的value值是否存在
    mapState.get("sensor_1")//获取key值
    mapState.put("sensor_1",1.0)//添加kv对

    reduceState.get()
    reduceState.add(in)

    in.id
  }
}
