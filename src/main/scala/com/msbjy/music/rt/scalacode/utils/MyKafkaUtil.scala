package com.msbjy.music.rt.scalacode.utils

import java.lang
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
  *  自定义Kafka工具类
  */
object MyKafkaUtil {

  /**
    *  从指定Kafka topic 读取数据
    */
  def GetDataFromKafka(topic:String,props:Properties) = {
    new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props)
  }

  /**
    *  向指定Kafka topic 写入数据 ,这里写出数据key 为null
    */
  def WriteDataToKafkaWithOutKey(topic:String,props: Properties)={
    new FlinkKafkaProducer[String](topic,new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte],Array[Byte]](topic,null,element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)


  }
}
