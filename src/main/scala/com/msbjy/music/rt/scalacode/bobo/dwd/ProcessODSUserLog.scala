package com.msbjy.music.rt.scalacode.bobo.dwd

import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.ConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, FlinkKafkaProducer011, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 读取kafka ODS -ODS_LOG_USERLOG --用户日志数据 清洗到不同的dwd的topic
  */
object ProcessODSUserLog {
  private val odsloguserlogtopic: String = "ODS_LOG_USERLOG"
  private val dwdsongplayinforttopic: String = "DWD_SONG_PLAY_INFO_RT"
  private val dwdotheruserlogrttopic: String = "DWD_OTHER_USERLOG_RT"

  def main(args: Array[String]): Unit = {
    // 创建环境 flink
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 导入隐士转换
    import org.apache.flink.streaming.api.scala._

    //服务kafka ods数据
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx1")

    val userLog: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](odsloguserlogtopic,new SimpleStringSchema(), props).setStartFromEarliest())


    //对不同类型数据写入DWD topic

    val ot = new OutputTag[String]("other-log")
    val mainDs: DataStream[String] = userLog.keyBy(line => {
      JSON.parseObject(line).getString("event_type")
    }).process(new KeyedProcessFunction[String, String, String] {
      override def processElement(jsonStr: String, context: KeyedProcessFunction[String, String, String]#Context, collector: Collector[String]) = {
        val obj: JSONObject = JSON.parseObject(jsonStr)
        val eventType: String = obj.getString("event_type")
        if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(eventType)) {
          //用户点播歌曲，主流
          val eventContent: String = obj.getString("event_content")
          collector.collect(eventContent)
        } else {
          //测流
          context.output(ot, jsonStr)
        }
      }
    })


    //5.将结果写入DWD层 topic
    mainDs.addSink(new FlinkKafkaProducer[String](dwdsongplayinforttopic, new KafkaSerializationSchema[String] {
      override def serialize(current: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val mid: String = JSON.parseObject(current).getString("mid")
        new ProducerRecord[Array[Byte], Array[Byte]](dwdsongplayinforttopic,mid.getBytes(),current.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    mainDs.getSideOutput(ot).addSink(new FlinkKafkaProducer[String](dwdotheruserlogrttopic, new KafkaSerializationSchema[String] {
      override def serialize(current: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](dwdotheruserlogrttopic,null, current.getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    mainDs.print("主流数据")
    mainDs.getSideOutput(ot).print("测流数据")

    env.execute()
  }
}
