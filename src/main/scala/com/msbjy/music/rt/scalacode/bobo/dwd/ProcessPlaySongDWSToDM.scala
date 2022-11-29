package com.msbjy.music.rt.scalacode.bobo.dwd

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.jdbc.{JDBCAppendTableSink}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

//实例对象
case class PlaySongInfo(songid:String,songname:String,singername:String,dataDt:Long)

//向ClickHouse中保存的对象
case class SaveClickHousePlaySongInfo(songid:String,songname:String,singername:String,playtimes:Long,dataDt:String)

/**
  * DWS 层DWS_SONG_PLAY_INFO_WIDE_RT 用flink写入ck
  */
object ProcessPlaySongDWSToDM {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx10")

    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWS_SONG_PLAY_INFO_WIDE_RT", new SimpleStringSchema(),props).setStartFromEarliest())


    val transDS: DataStream[PlaySongInfo] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      PlaySongInfo(nObject.getString("songid"), nObject.getString("songname"), nObject.getString("singer_info"), nObject.getLongValue("time"))
    })

    //设置watermrk
    val tranDS2: DataStream[PlaySongInfo] = transDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PlaySongInfo](Time.seconds(5)) {
      override def extractTimestamp(ele: PlaySongInfo): Long = {
        ele.dataDt
      }
    })

    //设置窗口统计
    val result: DataStream[SaveClickHousePlaySongInfo] = tranDS2.keyBy(playSongInfo => {
      playSongInfo.songid + ":" + playSongInfo.songname + ":" + playSongInfo.singername
    }).timeWindow(Time.seconds(10)).aggregate(new AggregateFunction[PlaySongInfo, Long, Long] {
      //初始化累加器
      override def createAccumulator() = 0L

      //累加器累加值
      override def add(in: PlaySongInfo, acc: Long) = acc + 1

      //返回结果
      override def getResult(acc: Long) = acc

      //合并累加器,不同分区的累加器
      override def merge(acc: Long, acc1: Long) = acc + acc1
    }, new WindowFunction[Long, SaveClickHousePlaySongInfo, String, TimeWindow] {
      //处理成返回对象的值
      override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[SaveClickHousePlaySongInfo]): Unit = {
        val splits: Array[String] = key.split(":")
        val start: Long = window.getStart
        val end: Long = window.getEnd
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dateDt: String = sdf.format(new Date(start))
        out.collect(SaveClickHousePlaySongInfo(splits(0), splits(1), splits(2), input.iterator.next(), dateDt))
      }
    })
    // clickhouse保存
    val table: Table = tableEnv.fromDataStream(result)

    table.printSchema()

    //注册clickhouse sink对象
    val clickHouseSink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
      .setDBUrl("jdbc:clickhouse://hadoop104:8123/default")
      .setUsername("default")
      .setPassword("")
      .setQuery("insert into dm_songplay_hot values (?,?,?,?,?)")
      .setBatchSize(3)
      .setParameterTypes(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.STRING)
      .build()
    //注册table sink
    tableEnv.registerTableSink("clickhouse-sink",clickHouseSink.configure(
      Array[String]("songid","songname","singername","playtimes","dataDt"),
      Array(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.STRING)
    ))

    tableEnv.insertInto(table, "clickhouse-sink")

    env.execute()
  }
}
