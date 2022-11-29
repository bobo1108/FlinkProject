package com.msbjy.music.rt.scalacode

import java.util.Properties

import com.msbjy.music.rt.scalacode.utils.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}

object TempCode {
  def main(args: Array[String]): Unit = {
//    val nObject = new JSONObject()
//    nObject.put("a",100)
//    nObject.put("b",200)
//    nObject.put("c",300)
//    println(nObject.toString)
//
//    nObject.put("a",2000)
//
//    println(nObject.toString)

    println("You''re Not Alone".replace("'","\\'"))



//    val s = new StringBuffer("hello")
//    println(s.deleteCharAt(s.length()-1))
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    import org.apache.flink.streaming.api.scala._
//    val props = new Properties()
//    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
//    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
//    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
//    props.setProperty("group.id","xx1")
//    val ds: DataStream[String] = env.addSource(MyKafkaUtil.GetDataFromKafka("ODS_DB_BUSSINESS_DATA",props))
//    ds.filter(line=>{
//      val nObject: JSONObject = JSON.parseObject(line)
//      val str: String = nObject.getString("table")
//      "machine_consume_detail".equals(str)
//    }).print()
//    env.execute()
  }

}
