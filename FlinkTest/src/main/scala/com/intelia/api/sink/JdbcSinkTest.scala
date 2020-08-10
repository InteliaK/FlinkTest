package com.intelia.api.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.intelia.api.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @description TODO
 * @author Intelia
 * @date 2020.8.7 22:00
 * @version
 */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inpath = "D:\\work_study\\workspace\\gmall_parent\\FlinkTest\\src\\main\\resources\\Flink.txt"
    val inputStream: DataStream[String] = env.readTextFile(inpath)

    val dataStream = inputStream
      .map{
        data =>
          val datas: Array[String] = data.split(",")
          SensorReading(datas(0),datas(1).toLong,datas(2).toDouble)
      }

    dataStream.addSink(new MyJdbcSinkFunc)

    env.execute()
  }

}
class MyJdbcSinkFunc extends RichSinkFunction[SensorReading]{
  //定义连接，预编译语句
  var connect : Connection = _
  var insertStmt : PreparedStatement = _
  var upsetStmt : PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    connect = DriverManager.getConnection("jdbc:mysql://hadoop201:3306/gmall0213_rs","root","123456")
    insertStmt = connect.prepareStatement("insert into  sensor_temp (id,temp) values(?,?)")
    upsetStmt = connect.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }


  override def invoke(value: SensorReading): Unit = {
    upsetStmt.setDouble(1,value.temperature)
    upsetStmt.setString(2,value.id)
    upsetStmt.execute()
    if(upsetStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    upsetStmt.close()
    connect.close()
  }

}