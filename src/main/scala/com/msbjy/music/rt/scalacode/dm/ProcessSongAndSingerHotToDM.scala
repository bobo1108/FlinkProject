package com.msbjy.music.rt.scalacode.dm

import java.util.Properties

import com.msbjy.music.rt.scalacode.utils._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

/**
  *  根据DWS层 “DWS_SONG_PLAY_INFO_WIDE_RT” 实时点播歌曲数据，统计歌曲和歌手任务
  *  思路如下：
  *  1.读取Kafka DWS层 “DWS_SONG_PLAY_INFO_WIDE_RT”数据
  *  2.设置并行度，可以设置下watermark
  *  3.设置key为歌曲id、歌曲、歌手，设置窗口每5s统计结果写出到ClickHouse 表 “dm_songplay_hot”中
  */
object ProcessSongAndSingerHotToDM {
    private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
    private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
    private val dwsSongPlayInfoWideRtTopic: String = ConfigUtil.DWS_SONG_PLAY_INFO_WIDE_RT_TOPIC
    private val chTblDmSongPlayHot: String = ConfigUtil.CHTBL_DM_SONGPLAY_HOT
    var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv  = StreamTableEnvironment.create(env)

    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    //2.设置读取Kafka并行度
    env.setParallelism(3)

    //3.设置事件时间，在flink1.11之后，默认为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //4.设置Kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","dws_song_playinfo_wide_rt_group")

    //5.从数据中获取Kafka DWD层 数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsSongPlayInfoWideRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsSongPlayInfoWideRtTopic,props))
    }

    //6.为了后期操作方便，这里将Json格式数据转换成对象
    /**
      * {
      *   "optrate_type": 0,
      *   "dur_time": 0,
      *   "mid": 40933,
      *   "session_id": 1031,
      *   "songname": "年少有为",
      *   "play_time": 0,
      *   "uid": 0,
      *   "singer_info": "李荣浩",
      *   "consume_type": 0,
      *   "time": 1620985690304,
      *   "pkg_id": 100,
      *   "songid": "lx149965",
      *   "order_id": ""
      * }
      */
    val playSongInfo: DataStream[PlaySongInfo] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      PlaySongInfo(nObject.getString("optrate_type"), nObject.getLongValue("dur_time"), nObject.getString("mid"),
        nObject.getString("session_id"), nObject.getString("songname"), nObject.getLongValue("play_time"),
        nObject.getString("uid"), nObject.getString("singer_info"), nObject.getString("consume_type"),
        nObject.getLongValue("time"), nObject.getString("pkg_id"), nObject.getString("songid"), nObject.getString("order_id"))
    })

    //6.设置watermark 允许迟到3s,设置事件时间字段
    val transDS: DataStream[PlaySongInfo] = playSongInfo.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PlaySongInfo](Time.seconds(3)) {
      override def extractTimestamp(element: PlaySongInfo): Long = element.time
    })

    //7.设置key,key为“歌曲id:歌曲名称：歌手名称”,并设置窗口，进行统计分析
    val songAndSingerHotInfoDS: DataStream[SongAndSingerHotInfo] = transDS.keyBy(songPalyInfo => {
      s"${songPalyInfo.songId}:${songPalyInfo.songName}:${songPalyInfo.singerInfo}"
    })
      .timeWindow(Time.seconds(5))
      //设置tuple类型累加 （歌曲点播量，歌手点播量）
      .aggregate(new AggregateFunction[PlaySongInfo, (Long, Long), (Long, Long)] {
      override def createAccumulator(): (Long, Long) = (0L, 0L)

      override def add(value: PlaySongInfo, accumulator: (Long, Long)): (Long, Long) = (accumulator._1 + 1, accumulator._2 + 1)

      override def getResult(accumulator: (Long, Long)): (Long, Long) = accumulator

      override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = (a._1 + b._1, a._2 + b._2)
    }, new WindowFunction[(Long, Long), SongAndSingerHotInfo, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(Long, Long)], out: Collector[SongAndSingerHotInfo]): Unit = {
        //获取窗口开始时间,转换成“yyyy-MM-dd”格式
        val dataDt: String = DateUtil.getDateYYYYMMDD(window.getStart)
        val split: Array[String] = key.split(":")
        ////song_id,song_name,singer_name,song_playtimes,singer_playtimes,data_dt
        out.collect(SongAndSingerHotInfo(split(0), split(1), split(2), input.iterator.next()._1, input.iterator.next()._2, dataDt,
          DateUtil.getDateYYYYMMDDHHMMSS(window.getStart), DateUtil.getDateYYYYMMDDHHMMSS(window.getEnd)))
      }
    })

    //8.将结果写入ClickHouse,由于Flink1.10.x版本前只支持Table Api向ClickHouse中写入数据，所以这里将结果数据组织成Table写出
    val table: Table = tableEnv.fromDataStream(songAndSingerHotInfoDS)
    table.printSchema()

    //9.将结果写入ClickHouse
    /**
      *   CREATE TABLE dm_songplay_hot(
      *   	song_id String comment '歌曲id',
      *   	song_name String comment '歌曲名称',
      *   	singer_name String comment '歌手名称',
      *   	song_playtimes Int32 comment '歌曲点播次数',
      *   	singer_playtimes Int32 comment '歌手点播次数',
      *   	data_dt String comment '数据日期'
      *   ) ENGINE = SUMMINGMERGETREE(song_playtimes,singer_playtimes)
      *   ORDER BY (song_id,song_name,singer_name)
      *   PARTITION BY data_dt;
      */
    MyClickHouseUtil.insertIntoClickHouse(
      tableEnv,
      table.select('songId,'songName,'singerName,'songPlayTimes,'singerPlayTimes,'dataDt),
      "dm_songplay_hot",//这个可以随意传值，这里为了便于理解，写上表名
      s"insert into ${chTblDmSongPlayHot} values (?,?,?,?,?,?)",
      Array[String]("song_id","song_name","singer_name","song_playtimes","singer_playtimes","data_dt"),
      //注意，数据类型一定和Flink 查出的数据类型一致，不然报错，不是和clickHouse表中的类型一致
      Array(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.STRING)
    )

    env.execute()

  }

}
