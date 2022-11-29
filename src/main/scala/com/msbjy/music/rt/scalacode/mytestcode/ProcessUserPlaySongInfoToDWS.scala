package com.msbjy.music.rt.scalacode.mytestcode

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyRedisUtil
import org.apache.kafka.clients.producer.ProducerRecord

/**
  *  处理Kafak DWD层-DWD_SONG_PLAY_INFO_RT 数据，与HBase关联，数据拉宽到DWS-kafka
  */
object ProcessUserPlaySongInfoToDWS {

  def main(args: Array[String]): Unit = {
    //1.创建Flink 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3.读取Kafka ODS数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx4")
    val userPlaySongLogDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWD_SONG_PLAY_INFO_RT",
      new SimpleStringSchema(),props).setStartFromEarliest() )

    val result: DataStream[String] = userPlaySongLogDS.process(new ProcessFunction[String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:phoenix:node3,node4,hadoop102:2181")
      }

      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        val nObject: JSONObject = JSON.parseObject(value)
        val songID: String = nObject.getString("songid")
        //向Redis中查询有没有当前歌曲id对应的数据，如果有从Redis中获取数据，如果没有从phx中获取
        val redisCacheInfo: String = MyRedisUtil.getInfoFromRedisCache("dim_song",songID)

        if(redisCacheInfo==null){
          //要从Phx中查询数据
          println("从phoenix 中查询数据")

          pst = conn.prepareStatement(s"select source_id,name,album,singer_info,post_time,authorized_company from dim_song where SOURCE_ID = '${songID}' ")
          val rst: ResultSet = pst.executeQuery()
          var source_id: String = "无信息"
          var name: String ="无信息"
          var album: String = "无信息"
          var singer_info: String ="无信息"
          var post_time: String ="无信息"
          var authorized_company: String ="无信息"

          while (rst.next()) {
            source_id = rst.getString("source_id")
            name = rst.getString("name")
            album = rst.getString("album")
            singer_info = rst.getString("singer_info")
            post_time = rst.getString("post_time")
            authorized_company = rst.getString("authorized_company")

          }
          nObject.put("name", name)
          nObject.put("album", album)
          nObject.put("singer_info", singer_info)
          nObject.put("post_time", post_time)
          nObject.put("authorized_company", authorized_company)

          //创建向Redis中保存的json对象
          val json = new JSONObject()
          json.put("source_id",source_id)
          json.put("name",name)
          json.put("album",album)
          json.put("singer_info",singer_info)
          json.put("post_time",post_time)
          json.put("authorized_company",authorized_company)

          //设置缓存：向Redis中存储数据
          MyRedisUtil.setRedisDimCache("dim_song",songID,json.toString)

        }else{
          //从Redis 获取数据
          println("从Redis中查询数据")
          val cacheInfo: JSONObject = JSON.parseObject(redisCacheInfo)
          nObject.put("name", cacheInfo.getString("name"))
          nObject.put("album", cacheInfo.getString("album"))
          nObject.put("singer_info", cacheInfo.getString("singer_info"))
          nObject.put("post_time", cacheInfo.getString("post_time"))
          nObject.put("authorized_company", cacheInfo.getString("authorized_company"))

        }

        out.collect(nObject.toString)
      }
    })

    //写入Kafka DWS topic ....
    result.addSink(new FlinkKafkaProducer[String]("DWS_SONG_PLAY_INFO_WIDE_RT",new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new  ProducerRecord[Array[Byte], Array[Byte]]("DWS_SONG_PLAY_INFO_WIDE_RT",null,element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    env.execute()

  }
}
