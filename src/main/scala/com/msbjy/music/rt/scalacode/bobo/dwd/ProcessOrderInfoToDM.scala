package com.msbjy.music.rt.scalacode.bobo.dwd

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyClickHouseUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

case class OrderInfo(province:String,city:String,address:String,mid:String,machine_name:String,pkg_name:String,amount:Double,inv_rate:Double,
                     age_rate:Double,com_rate:Double,par_rate:Double,action_time:Long)

case class CHOrderInfo(province:String,city:String,address:String,mid:String,machine_name:String,pkg_name:String,amount:Double,inv_rev:Double,
                       age_rev:Double,com_rev:Double,par_rev:Double,data_dt:String)
/**
  * Create by Thinkpad on 2022/11/15.
  */
object ProcessOrderInfoToDM {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx3")

    val ds = env.addSource(new FlinkKafkaConsumer[String]("DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT",new SimpleStringSchema()
      ,props).setStartFromEarliest())

    //处理宽表数据
    val ds2: DataStream[OrderInfo] = ds.map(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      OrderInfo(nObject.getString("province"), nObject.getString("city"),
        nObject.getString("address"),
        nObject.getString("mid"),
        nObject.getString("machine_name"),
        nObject.getString("pkg_name"),
        nObject.getString("amount").toDouble,
        nObject.getString("inv_rate").toDouble,
        nObject.getString("age_rate").toDouble,
        nObject.getString("com_rate").toDouble,
        nObject.getString("par_rate").toDouble,
        nObject.getString("action_time").toLong
      )
    })
    val result: DataStream[CHOrderInfo] = ds2.process(new ProcessFunction[OrderInfo, CHOrderInfo] {
      override def processElement(value: OrderInfo, context: ProcessFunction[OrderInfo, CHOrderInfo]#Context, out: Collector[CHOrderInfo]) = {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val data_dt: String = sdf.format(new Date(value.action_time))
        out.collect(CHOrderInfo(value.province, value.city, value.address, value.mid, value.machine_name, value.pkg_name, value.amount,
          value.amount * value.inv_rate / 100, value.amount * value.age_rate / 100, value.amount * value.com_rate / 100, value.amount * value.par_rate / 100, data_dt))
      }
    })
    //result.print()
    val table: Table = tableEnv.fromDataStream(result)
    table.printSchema()
    //结果写入clickhouse
    /**
      * create table dm_order_info_rt(
      *  province String,
      *  city String,
      *  address String,
      *  mid String,
      *  machine_name String,
      *  pkg_name String,
      *  amount Decimal(9,2),
      *  inv_rev Decimal(9,2),
      *  age_rev Decimal(9,2),
      *  com_rev Decimal(9,2),
      *  par_rev Decimal(9,2),
      *  data_dt String
      * ) engine = MergeTree()
      * order by (province,city)
      * partition by data_dt
      */
    MyClickHouseUtil.insertIntoClickHouse(tableEnv, table, "xxx",
    "insert into table dm_order_info_rt values (?,?,?,?,?,?,?,?,?,?,?,?)",
      Array[String]("province","city","address","mid","machine_name","pkg_name","amount","inv_rev","age_rev","com_rev","par_rev","data_dt"),
      Array(Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.DOUBLE,Types.STRING))

    env.execute()
  }

}
