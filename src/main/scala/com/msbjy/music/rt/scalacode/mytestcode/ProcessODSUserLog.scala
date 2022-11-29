package com.msbjy.music.rt.scalacode.mytestcode

import java.lang
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.ConfigUtil
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 读取Kafka ODS - ODS_LOG_USERLOG  -- 用户日志数据 数据清洗到不同的DWD层topic
  *
  */
object ProcessODSUserLog {
  private val odsloguserlogtopic: String = ConfigUtil.ODS_LOG_USERLOG_TOPIC
  private val dwdsongplayinforttopic: String = ConfigUtil.DWD_SONG_PLAY_INFO_RT_TOPIC
  private val dwdotheruserlogrttopic: String = ConfigUtil.DWD_OTHER_USERLOG_RT_TOPIC

  def main(args: Array[String]): Unit = {
    //1.创建Flink 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.导入隐式转换ODS_LOG_USERLOG
    import org.apache.flink.streaming.api.scala._

    //3.读取Kafka ODS数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx1")
    val userLogDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](odsloguserlogtopic,
      new SimpleStringSchema(),props)/*.setStartFromEarliest()*/ )

    //4.对不同类型数据写入不同DWD层 topic
    //创建侧流标签
    val ot = new OutputTag[String]("other-log")
    val mainDS: DataStream[String] = userLogDS.keyBy(line => {
      JSON.parseObject(line).getString("event_type")
    }).process(new KeyedProcessFunction[String, String, String] {
      override def processElement(jsonStr: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val nObject: JSONObject = JSON.parseObject(jsonStr)
        val eventType: String = nObject.getString("event_type")
        if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(eventType)) {
          //用户点播歌曲日志 ，放主流
          val eventContent: String = nObject.getString("event_content")
          out.collect(eventContent)
        } else {
          //其他日志 ，放侧流
          ctx.output(ot, jsonStr)
        }
      }
    })

    //5.将结果写入DWD层 topic
    //{
    //		"songid": "LX_mk111944",
    //		"mid": 89923,
    //		"optrate_type": 0,
    //		"uid": 49915658,
    //		"consume_type": 0,
    //		"play_time": 0,
    //		"dur_time": 0,
    //		"session_id": 13126,
    //		"songname": "一次就好",
    //		"pkg_id": 100,
    //		"order_id": ""
    //	}
    mainDS.addSink(new FlinkKafkaProducer[String](dwdsongplayinforttopic,new KafkaSerializationSchema[String] {
      override def serialize(currentLog: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val mid: String = JSON.parseObject(currentLog).getString("mid")
        new ProducerRecord[Array[Byte], Array[Byte]](dwdsongplayinforttopic,mid.getBytes(),currentLog.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


    mainDS.getSideOutput(ot).addSink(new FlinkKafkaProducer[String](dwdotheruserlogrttopic,new KafkaSerializationSchema[String] {
      override def serialize(currentLog: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](dwdotheruserlogrttopic,null,currentLog.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


//    mainDS.print("主流-用户日志数据")
//    mainDS.getSideOutput(ot).print("侧流-其他日志")

    env.execute()


  }
}
