package com.msbjy.music.rt.scalacode.dm

import java.util.Properties

import com.msbjy.music.rt.scalacode.utils._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table

/**
  * 针对Kafka DWS层“DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT”数据，设置窗口每5s统计一次数据，
  * 结果写入DM clickhouse 表 dm_order_info_rt 中。
  */
object ProcessMachineConsumerInfoToDM {

  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val dwsMachineConsumerDetailWideRtTopic: String = ConfigUtil.DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT
  private val chTblDmOrderInfoRt: String = ConfigUtil.CHTBL_DM_ORDER_INFO_RT

  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv  = StreamTableEnvironment.create(env)

    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._

    //2.设置读取Kafka并行度
    env.setParallelism(3)

//    //3.设置事件时间，在flink1.11之后，默认为事件时间
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //4.设置Kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","dws_song_playinfo_wide_rt_group")

    //5.从数据中获取Kafka DWD层 数据
//    if(consumeKafkaFromEarliest){
    if(true){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsMachineConsumerDetailWideRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwsMachineConsumerDetailWideRtTopic,props))
    }

    //5.为了操作方便将Json格式数据转换为对象
    val machineOrderDetailDS: DataStream[MachineOrderDetail] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      //case class MachineOrderDetail(province:String,city:String,groundName:String,sceneAddress:String,mid:String,machineName:String,
      //  pkgName:String,invRate:Double,ageRate:Double,comRate:Double,parRate:Double,billDate:String,actionTime:Long)
      MachineOrderDetail(nObject.getString("province"), nObject.getString("city"), nObject.getString("ground_name"),
        nObject.getString("scene_address"), nObject.getString("mid"), nObject.getString("machine_name"),
        nObject.getString("pkg_name"), nObject.getDoubleValue("amount"), nObject.getDoubleValue("inv_rate")/100,
        nObject.getDoubleValue("age_rate")/100, nObject.getDoubleValue("com_rate")/100, nObject.getDoubleValue("par_rate")/100,
        nObject.getString("bill_date"), nObject.getLongValue("action_time"))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MachineOrderDetail](Time.seconds(3)) {
      override def extractTimestamp(element: MachineOrderDetail): Long = element.actionTime
    })
    //6.将数据按照key分组，设置窗口，key :
    val result: DataStream[MachineRevDetail] = machineOrderDetailDS.map(mod => {
      //case class MachineRevDetail(province:String,city:String,address:String,mid:String,machineName:String,pkgName:String,
      // amount:Double,invRev:Double,ageRev:Double,comRev:Double,parRev:Double,dataDt:String)
      println("mod = "+mod)
      MachineRevDetail(mod.province, mod.city, mod.sceneAddress + "" + mod.groundName, mod.mid, mod.machineName, mod.pkgName, mod.amount,
        mod.invRate * mod.amount, mod.ageRate * mod.amount, mod.comRate * mod.amount, mod.parRate * mod.amount, mod.billDate)
    })

    //7.将结果写入ClickHouse,由于Flink1.10.x版本前只支持Table Api向ClickHouse中写入数据，所以这里将结果数据组织成Table写出
    val table: Table = tableEnv.fromDataStream(result)
    table.printSchema()

    //9.将结果写入ClickHouse
    /**
      * CREATE TABLE dm_order_info_rt(
      *   province String comment '省份',
      *   city String comment '城市',
      *   address String comment '详细地址',
      *   mid String comment '机器id',
      *   machine_name String comment '机器名称',
      *   pkg_name String comment '下单套餐',
      *   amount Decimal(9,2) comment '营收或退款',
      *   inv_rev Decimal(9,2) comment '投资人收益',
      *   age_rev Decimal(9,2) comment '承接方收益',
      *   com_rev Decimal(9,2) comment '公司收益',
      *   par_rev Decimal(9,2) comment '合作方收益',
      *   data_dt String comment '数据日期'
      * ) ENGINE = MergeTree()
      * ORDER BY (province,city)
      * PARTITION BY data_dt;
      *
      */
    MyClickHouseUtil.insertIntoClickHouse(
      //case class MachineRevDetail(province:String,city:String,address:String,mid:String,machineName:String,pkgName:String,
      // amount:Double,invRev:Double,ageRev:Double,comRev:Double,parRev:Double,dataDt:String)
      tableEnv,
      table.select('province,'city,'address,'mid,'machineName,'pkgName,'amount,'invRev,'ageRev,'comRev,'parRev,'dataDt),
      "dm_order_infoxxx",//这个可以随意传值，这里为了便于理解，写上表名
      s"insert into ${chTblDmOrderInfoRt} values (?,?,?,?,?,?,?,?,?,?,?,?)",
      Array[String]("province","city","address","mid","machineName","pkgName","amount","invRev","ageRev","comRev","parRev","dataDt"),
      //注意，数据类型一定和Flink 查出的数据类型一致，不然报错，不是和clickHouse表中的类型一致
      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
        Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.STRING)
    )

    env.execute("clickhouse 保存成功")


  }

}
