package com.msbjy.music.rt.scalacode.dws

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.msbjy.music.rt.scalacode.utils._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.configuration.Configuration

/**
  * 由 Kafka DWD “DWD_SONG_PLAY_INFO_RT”用户点播歌曲日志数据
  * 结合HBase中的维度数据获取用户点播歌曲宽表数据，存入Kafka DWS 层 “DWS_SONG_PLAY_INFO_RT”
  *
  * Flink代码步骤如下：
  * 1.Flink 读取Kafka DWD层“DWD_SONG_PLAY_INFO_RT”用户点播歌曲日志数据
  * 2.从Redis中获取事件流中歌曲的基本信息
  * 3.如果从Redis中获取到，那么就结合流事件组织宽表数据写出到DWS 层 DWS_SONG_PLAY_INFO_WIDE_RT
  * 4.如果Redis中获取不到，那么就通过Phoenix读取HBase获取，组织宽表数据写出到DWS 层 DWS_SONG_PLAY_INFO_WIDE_RT
  *   同时向Redis缓存中设置数据，并且设置过期时间为一天
  *
  *   注意：这里还需要考虑维度变化时，将Redis数据删除，需要在代码“DimDataToHBase.scala”中设置
  */
object ProcessUserPlaySongDataToDWS {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val dwdSongPlayInfoRtTopic: String = ConfigUtil.DWD_SONG_PLAY_INFO_RT_TOPIC
  private val hbaseDimSong: String = ConfigUtil.HBASE_DIM_SONG
  private val dwsSongPlayInfoWideRtTopic: String = ConfigUtil.DWS_SONG_PLAY_INFO_WIDE_RT_TOPIC
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //2.设置读取Kafka并行度
    env.setParallelism(3)

    //3.设置Kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","dwd_song_playinfo_rt_group")

    //4.从数据中获取Kafka DWD层 数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdSongPlayInfoRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdSongPlayInfoRtTopic,props))
    }

    val dwsDs: DataStream[String] = ds.process(new ProcessFunction[String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _
      var rs: ResultSet = _

      //开启Phoenix连接
      override def open(parameters: Configuration): Unit = {
        //连接Phoenix
        println(s"连接Phoenix ... ...")
        conn = DriverManager.getConnection(ConfigUtil.PHOENIX_URL)
      }

      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        //获取事件流中事件，转换为json对象
        //{"uid":5020324,"optrate_type":1,"dur_time":271,"mid":89923,"consume_type":0,"session_id":13124,
        // "time":1620477184175,"pkg_id":100,"songid":"LX_M001171","order_id":"","songname":"那个男人","play_time":161}
        val nObject: JSONObject = JSON.parseObject(value)
        //获取歌曲id
        val songId: String = nObject.getString("songid")

        //根据歌曲id从Redis中获取歌曲基本信息缓存数据
        val dimJsonStr: String = MyRedisUtil.getInfoFromRedisCache(hbaseDimSong, songId)

        if (MyStringUtil.isEmpty(dimJsonStr)) {
          println("连接Phoenix查询维度数据")
          //为空说明Redis缓存中没有查询到当前歌曲缓存基本信息，从Phoenix中查询
          val sql = s"select source_id,name,album,singer_info,post_time,authorized_company from dim_song where source_id = '${songId}'"

          pst = conn.prepareStatement(sql)
          rs = pst.executeQuery()

          var sourceId: String = "无信息"
          var name: String = "无信息"
          var album: String = "无信息"
          var singerInfo: String = "无信息"
          var postTime: String = "无信息"
          var authorizedCompany: String = "无信息"
          while (rs.next()) {
            sourceId = rs.getString("source_id")
            name = rs.getString("name")
            album = rs.getString("album")
            singerInfo = rs.getString("singer_info")
            postTime = rs.getString("post_time")
            authorizedCompany = rs.getString("authorized_company")
          }

          //组织json串，将此条数据设置到Redis中
          val songDimJsonStr = new JSONObject()
          songDimJsonStr.put("source_id", sourceId)
          songDimJsonStr.put("name", name)
          songDimJsonStr.put("album", album)
          songDimJsonStr.put("singer_info", singerInfo)
          songDimJsonStr.put("post_time", postTime)
          songDimJsonStr.put("authorized_company", authorizedCompany)


          //向Redis中设置数据缓存
          MyRedisUtil.setRedisDimCache(hbaseDimSong, songId, songDimJsonStr.toString)

          //向当前事件流对应的json对象中设置 “singer_info”
          nObject.put("singer_info", songDimJsonStr.getString("singer_info"))
          //返回数据
          out.collect(nObject.toString)

        } else {
          println("从Redis中查询维度数据")
          //不为空，说明Redis中有查询到当前歌曲缓存基本信息
          val singerInfo: String = JSON.parseObject(dimJsonStr).getString("singer_info")
          //设置返回数据
          nObject.put("singer_info", singerInfo)
          out.collect(nObject.toString)
        }

      }

      override def close(): Unit = {
        rs.close()
        pst.close()
        conn.close()
      }
    })

    dwsDs.addSink(MyKafkaUtil.WriteDataToKafkaWithOutKey(dwsSongPlayInfoWideRtTopic,props))

    env.execute()

  }
}
