package com.msbjy.music.rt.scalacode.mytestcode

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


//实例对象
case class PlaySongInfo(songid:String,songname:String,singername:String,dataDt:Long)

//向ClickHouse中保存的对象
case class SaveClickHousePlaySongInfo(songid:String,songname:String,singername:String,playtimes:Long,dataDt:String)


// 将DWS 层 DWS_SONG_PLAY_INFO_WIDE_RT -- 点播歌曲宽表数据 使用flink 处理写入ck
object ProcessPlaySongDWSToDM {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx10")


    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWS_SONG_PLAY_INFO_WIDE_RT",new SimpleStringSchema(),props)
//    .setStartFromEarliest()
    )

    val transDS: DataStream[PlaySongInfo] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      PlaySongInfo(nObject.getString("songid"), nObject.getString("songname"), nObject.getString("singer_info"), nObject.getLongValue("time"))
    })

    //设置watermark
    val transDS2: DataStream[PlaySongInfo] = transDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PlaySongInfo](Time.seconds(5)) {
      override def extractTimestamp(element: PlaySongInfo): Long = {
        element.dataDt
      }
    })

    //keyby 后设置窗口统计
    val result: DataStream[SaveClickHousePlaySongInfo] = transDS2.keyBy(playSongInfo => {
      playSongInfo.songid + ":" + playSongInfo.songname + ":" + playSongInfo.singername
    }).timeWindow(Time.seconds(10)).aggregate(new AggregateFunction[PlaySongInfo, Long, Long] {
      override def createAccumulator(): Long = 0L

      override def add(value: PlaySongInfo, accumulator: Long): Long = accumulator + 1

      override def getResult(accumulator: Long): Long = accumulator

      override def merge(a: Long, b: Long): Long = a + b
    }, new WindowFunction[Long, SaveClickHousePlaySongInfo, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[SaveClickHousePlaySongInfo]): Unit = {
        val splits: Array[String] = key.split(":")

        val start: Long = window.getStart
        val end: Long = window.getEnd
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dataDt: String = sdf.format(new Date(start))

        //songid:String,songname:String,singername:String,playtimes:Long,dataDt:String
        out.collect(SaveClickHousePlaySongInfo(splits(0), splits(1), splits(2), input.iterator.next(),dataDt))
      }
    })

    //保存在clickhouse中
    //1.1 将DataStream转换成Table对象 -- StreamTableEnvironment
    val resultTbl: Table = tableEnv.fromDataStream(result)

    resultTbl.printSchema()

    //1.2 创建ClickHouse Sink 对象
    val clickHouseSink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
      .setDBUrl("jdbc:clickhouse://node1:8123/default")
      .setUsername("default")
      .setPassword("")
      .setQuery("insert into dm_songplay_hot values (?,?,?,?,?)")
      .setBatchSize(3)
      .setParameterTypes(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.STRING)
      .build()

    //1.3 注册table sink
    tableEnv.registerTableSink("clickhouse-sink",clickHouseSink.configure(
      Array[String]("songid","songname","singername","playtimes","dataDt"),
      Array(Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.STRING)
    ))

    //1.4 将Table 数据写入到ClickHouse Sink中
    tableEnv.insertInto(resultTbl,"clickhouse-sink")


    env.execute()





  }

}
