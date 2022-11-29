package com.msbjy.music.rt.scalacode.dm

import java.util.Properties

import com.msbjy.music.rt.scalacode.utils._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.Table
import org.apache.flink.util.Collector


/**
  * 读取Kafka DWS 层 “DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT”处理用户登录携带的上报位置topic信息，
  * 设置窗口，每5s输出对应的信息到DM层 ClickHouse 表“dm_user_login_loc_info_rt”中
  *
  */
object ProcessUseLoginDataToDM {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  private val dwsUserLoginLocationInfoRtTopic: String = ConfigUtil.DWS_USER_LOGIN_LOCATION_INFO_RT_TOPIC
  private val chTblDmUserLoginLocInfoRt: String = ConfigUtil.CHTBL_DM_USER_LOGIN_LOC_INFO_RT

  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv  = StreamTableEnvironment.create(env)

    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    //2.设置读取Kafka并行度
    env.setParallelism(1)

    //3.设置事件时间，在flink1.11之后，默认为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //4.设置Kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","dws_song_playinfo_wide_rt_group")

    //5.从数据中获取Kafka DWD层 数据
//    if(consumeKafkaFromEarliest){
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsUserLoginLocationInfoRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsUserLoginLocationInfoRtTopic,props))
    }

    //6.为了后期方便操作，这里将json格式数据转换成对象
    val userLoginInfoDS: DataStream[UserLoginInfo] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      //case class UserLoginInfo(uid:String,mid:String,province:String,city:String,district:String,address:String,lng:String,lat:String,loginDt:Long)
      UserLoginInfo(nObject.getString("uid"), nObject.getString("mid"), nObject.getString("province"), nObject.getString("city"),
        nObject.getString("district"), nObject.getString("address"), nObject.getString("lng"), nObject.getString("lat"),
        nObject.getLongValue("login_dt"))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserLoginInfo](Time.seconds(3)) {
      override def extractTimestamp(element: UserLoginInfo): Long = element.loginDt
    })

    //7.设置key ,province:city:district:lng:lat
    val endDS: DataStream[LoginLocTimesInfo] = userLoginInfoDS.keyBy(uli => {
      s"${uli.province}:${uli.city}:${uli.district}:${uli.lng}:${uli.lat}"
    }).timeWindow(Time.seconds(5))
      .aggregate(new AggregateFunction[UserLoginInfo, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: UserLoginInfo, accumulator: Long): Long = accumulator + 1L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      }, new WindowFunction[Long, LoginLocTimesInfo, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[LoginLocTimesInfo]): Unit = {
          val split: Array[String] = key.split(":")
          val provice: String = split(0)
          val city: String = split(1)
          val district: String = split(2)
          val lng: String = split(3)
          val lat: String = split(4)
          //获取登录次数
          val loginTimes: Long = input.iterator.next()
          //获取当前日期 "yyyy-MM-dd"
          val dataDt: String = DateUtil.getDateYYYYMMDD(window.getStart)
          out.collect(LoginLocTimesInfo(provice, city, district, lng, lat, loginTimes, dataDt))
        }
      })
    endDS.print("保存到ClickHouse数据>>>>>>")
    //8.保存到ClickHouse中
    val table: Table = tableEnv.fromDataStream(endDS)
    table.printSchema()

    //9.将结果写入ClickHouse
    /**
      *   CREATE TABLE dm_user_login_loc_info_rt(
      *   	province String comment '省份',
      *   	city String comment '城市',
      *   	district String comment '区县',
      *   	lng String comment '经度',
      *   	lat String comment '纬度',
      *   	login_times Int32 comment '登录次数',
      *   	data_dt String comment '数据日期'
      *   ) ENGINE = SummingMergeTree((province,city,district,lng,lat))
      *   ORDER BY (province)
      */
    MyClickHouseUtil.insertIntoClickHouse(
      tableEnv,
      table.select('province,'city,'district,'lng,'lat,'loginTimes,'loginDt),
      "dm_user_login_loc_info",//这个可以随意传值，这里为了便于理解，写上表名
      s"insert into ${chTblDmUserLoginLocInfoRt} values (?,?,?,?,?,?,?)",
      Array[String]("province","city","district","lng","lat","login_times","data_dt"),
      //注意，数据类型一定和Flink 查出的数据类型一致，不然报错，不是和clickHouse表中的类型一致
      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.STRING)
    )

    env.execute()

  }
}
