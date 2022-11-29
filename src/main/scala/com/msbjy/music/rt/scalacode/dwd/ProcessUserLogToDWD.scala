package com.msbjy.music.rt.scalacode.dwd

import java.lang
import java.util.Properties

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, MyKafkaUtil}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
/**
  *  针对 Kafka 中ODS层 “ODS_LOG_USERLOG” 用户日志数据进行清洗到DWD层
  */
object ProcessUserLogToDWD {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val odsLogUserLogTopic: String = ConfigUtil.ODS_LOG_USERLOG_TOPIC
  private val dwdSongPlayInfoRtTopic: String = ConfigUtil.DWD_SONG_PLAY_INFO_RT_TOPIC
  private val dwdUserLoginLocationInfoRtTopic: String = ConfigUtil.DWD_USER_LOGIN_LOCATION_INFO_RT_TOPIC
  private val dwdOtherUserLogRtTopic: String = ConfigUtil.DWD_OTHER_USERLOG_RT_TOPIC

  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //设置配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","ods_userlog_group")

    //从数据中指定数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsLogUserLogTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsLogUserLogTopic,props))
    }

    /**
      * 从数据流中获取对应“用户点播歌曲”事件写入Kafka “DWD_SONG_PLAY_INFO_RT”
      * 其他事件写入 Kafka “DWD_OTHER_USERLOG_RT” 备用，使用Flink侧输出流实现
      *
      * 流数据事件格式如下： "MINIK_CLIENT_SONG_PLAY_OPERATE_REQ" 此时间为用户点播歌曲事件
      * {"event_content":{"uid":0,"event_id":1,"src_verison":1575,"adv_type":4,"mid":83930,"session_id":3277,"src_type":1575,"time":1620470362998},
      * "event_type":"MINIK_CLIENT_ADVERTISEMENT_RECORD","mid":"83930","ui_version":"3.0.1.12","machine_version":"2.4.4.26","timestamp":1620470362998}
      */

    //定义侧流标签 - 非用户点播歌曲日志事件
    val otherLogs = new OutputTag[String]("other_logs")
    //定义侧流标签 - 用户登录经纬度位置信息事件
    val userLoginLocationLogs = new OutputTag[String]("user_login_location_log")


    //对事件流进行清洗，主流输出点播歌曲event_content对应的内容，侧流输出原始事件
    val end: DataStream[String] = ds.process(new ProcessFunction[String, String] {
      override def processElement(userLog: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        val jsonObj: JSONObject = JSON.parseObject(userLog)
        if ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ".equals(jsonObj.getString("event_type"))) {
          //用户点播歌曲日志事件,获取对应的event_content
          val userPlaySongLog: String = jsonObj.get("event_content").toString
          out.collect(userPlaySongLog)
        } else if("USER_LOGIN_LOCATION_LOG".equals(jsonObj.getString("event_type"))){
          //用户登录上报位置信息数据去掉“event_type"
          jsonObj.remove("event_type")
          ctx.output(userLoginLocationLogs,jsonObj.toString)
        }else{
          //非点播歌曲日志事件
          ctx.output(otherLogs, userLog)
        }
      }
    })

    //将主流数据写入Kafka“DWD_SONG_PLAY_INFO_RT” 中 ,这里考虑向Kafka 写数据时，指定相同用户数据去往同一分区
    end.print("user play song log >>>>>>>>>>")
    end.addSink(new FlinkKafkaProducer[String](dwdSongPlayInfoRtTopic,new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val nObject: JSONObject = JSON.parseObject(element)
        // uid当做Kafka中的key
        val key: String = nObject.getString("uid")
        new ProducerRecord[Array[Byte],Array[Byte]](dwdSongPlayInfoRtTopic,key.getBytes(),element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    //将侧流 用户登录上报经纬度位置信息 数据写入Kafka  “DWD_USER_LOGIN_LOCATION_INFO_RT”中
    end.getSideOutput(userLoginLocationLogs).print("user_login_location_info_rt userlog >>>>>>>>>>")
    end.getSideOutput(userLoginLocationLogs).addSink(new FlinkKafkaProducer[String](dwdUserLoginLocationInfoRtTopic,new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val nObject: JSONObject = JSON.parseObject(element)
        // uid当做Kafka中的key
        val key: String = nObject.getString("mid")
        new ProducerRecord[Array[Byte],Array[Byte]](dwdUserLoginLocationInfoRtTopic,key.getBytes(),element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


    //将侧其它流数据写入Kafka “DWD_OTHER_USERLOG_RT” 中，这里暂时不考虑 向kafka中写数据的key
    end.getSideOutput(otherLogs).print("other userlog >>>>>>>>>>")
    end.getSideOutput(otherLogs).addSink(MyKafkaUtil.WriteDataToKafkaWithOutKey(dwdOtherUserLogRtTopic,props))


    env.execute()
  }

}
