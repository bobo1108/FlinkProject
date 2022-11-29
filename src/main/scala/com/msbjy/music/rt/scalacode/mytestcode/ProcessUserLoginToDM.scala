package com.msbjy.music.rt.scalacode.mytestcode

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyClickHouseUtil
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector

case class UserLogInInfos(province:String,city:String,distinct:String,login_dt:Long,address:String)
case class ClickHouseRst(province:String,city:String,distinct:String,login_times:Long,data_dt:String)

//处理DWS层 用户登录宽表数据 到 DM ClickHouse中
object ProcessUserLoginToDM {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group2")
    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT",
      new SimpleStringSchema(),props))

    //转换成对象DS,设置事件时间
    val newDS: DataStream[UserLogInInfos] = ds.map(str => {
      val nObject: JSONObject = JSON.parseObject(str)
      UserLogInInfos(nObject.getString("province"), nObject.getString("city"), nObject.getString("district"),
        nObject.getLong("login_dt"), nObject.getString("address"))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLogInInfos](Time.seconds(5)) {
      override def extractTimestamp(element: UserLogInInfos): Long = element.login_dt
    })


    //设置窗口
    val result: DataStream[ClickHouseRst] = newDS.keyBy(userLongInInfos => {
      //省份-城市-区县
      userLongInInfos.province + "-" + userLongInInfos.city + "-" + userLongInInfos.distinct
    }).timeWindow(Time.seconds(10)).aggregate(new AggregateFunction[UserLogInInfos, Long, Long] {
      override def createAccumulator(): Long = 0L

      override def add(value: UserLogInInfos, accumulator: Long): Long = accumulator + 1L

      override def getResult(accumulator: Long): Long = accumulator

      override def merge(a: Long, b: Long): Long = a + b
    }, new WindowFunction[Long, ClickHouseRst, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[ClickHouseRst]): Unit = {
        val splits: Array[String] = key.split("-")
        val start: Long = window.getStart
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dataDT: String = sdf.format(new Date(start))
        out.collect(ClickHouseRst(splits(0), splits(1), splits(2), input.iterator.next(), dataDT))
      }
    })

    val table: Table = tableEnv.fromDataStream(result)
    table.printSchema()



    //将数据写ClickHouse中
    MyClickHouseUtil.insertIntoClickHouse(tableEnv,table,"xx",
    "insert into dm_user_login_loc_info_rt values (?,?,?,?,?)",
      Array[String]("province","city","distinct","login_times","data_dt"),
      Array(Types.STRING,Types.STRING,Types.STRING,Types.LONG,Types.STRING))

    env.execute()
  }

}
