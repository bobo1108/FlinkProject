package com.msbjy.music.rt.scalacode.bobo.dwd

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.dwd.ProcessDBLogToDWD.{consumeKafkaFromEarliest, ds, odsDbBussinessDataTopic}
import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, MyKafkaUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer

case class TblConfig(tblName:String,tblDB:String,tblType:String,pkCol:String,cols:String,phoenixTableName:String,sinkTopic:String)
/**
  * 读取kafka ODS ODS_DB_BUSSINESS_DATA 业务库数据，根据配置决定数据写往哪个topic
  */
object ProcessODSDBToDWD {
  private val odsDbBussinessDataTopic: String = ConfigUtil.ODS_DB_BUSSINESS_DATA_TOPIC
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx2")

    ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsDbBussinessDataTopic,props))
//    //4.从数据中获取Kafka ODS层 数据
//    if(consumeKafkaFromEarliest){
//      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsDbBussinessDataTopic,props).setStartFromEarliest())
//    }else{
//      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsDbBussinessDataTopic,props))
//    }

    ds.print("aaaaa")

    // configDS是Flink 读取mysql数据，做广播流
    val configDS: DataStream[TblConfig] = env.addSource(new RichSourceFunction[TblConfig] {
      var conn: Connection = _
      var pst: PreparedStatement = _
      var flag = true

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://hadoop104:3306/rt_songdb", "root", "123")
      }

      override def run(ctx: SourceFunction.SourceContext[TblConfig]) = {
        while (flag) {
          pst = conn.prepareStatement("select tbl_name,tbl_db,tbl_type,pk_col,cols,phoenix_tbl_name,sink_topic from tbl_config_info")
          val rst: ResultSet = pst.executeQuery()
          while (rst.next()) {
            val tableName: String = rst.getString("tbl_name")
            val tableDb: String = rst.getString("tbl_db")
            val tableType: String = rst.getString("tbl_type")
            val pk_col: String = rst.getString("pk_col")
            val cols: String = rst.getString("cols")
            val phoenix_tbl_name: String = rst.getString("phoenix_tbl_name")
            val sink_topic: String = rst.getString("sink_topic")
            ctx.collect(TblConfig(tableName, tableDb, tableType, pk_col, cols, phoenix_tbl_name, sink_topic))
          }

          Thread.sleep(60 * 1000)
        }
      }

      override def cancel() = {
        flag = false
      }

      override def close(): Unit = {
        pst.close()
        conn.close()
      }
    })

    //广播mysql流
    val msd = new MapStateDescriptor[String, TblConfig]("msd", classOf[String], classOf[TblConfig])
    val bcConfigDS: BroadcastStream[TblConfig] = configDS.broadcast(msd)

    val resultDs: DataStream[String] = ds.connect(bcConfigDS).process(new BroadcastProcessFunction[String, TblConfig, String] {
      //处理主流
      override def processElement(value: String, ctx: BroadcastProcessFunction[String, TblConfig, String]#ReadOnlyContext, out: Collector[String]) = {
        val bcDs: ReadOnlyBroadcastState[String, TblConfig] = ctx.getBroadcastState(msd)
        val nObject: JSONObject = JSON.parseObject(value)
        val db: String = nObject.getString("database")
        val tableName: String = nObject.getString("table")
        val tp: String = nObject.getString("type")
        val ts: String = nObject.getString("ts")
        if (bcDs.contains(db + ":" + tableName)) {
          val config: TblConfig = bcDs.get(db + ":" + tableName)
          val dataJson: JSONObject = nObject.getJSONObject("data")
          dataJson.put("ts", ts)
          dataJson.put("type", tp)
          dataJson.put("pk_col", config.pkCol)
          dataJson.put("cols", config.cols)
          dataJson.put("phoenix_table_name", config.phoenixTableName)
          dataJson.put("sink_topic", config.sinkTopic)
          out.collect(dataJson.toString)
        }
      }

      //处理广播流
      override def processBroadcastElement(value: TblConfig, ctx: BroadcastProcessFunction[String, TblConfig, String]#Context, collector: Collector[String]) = {
        val bs: BroadcastState[String, TblConfig] = ctx.getBroadcastState(msd)
        bs.put(value.tblDB + ":" + value.tblName, value)
      }
    })

    resultDs.addSink(new FlinkKafkaProducer[String]("xxx", new KafkaSerializationSchema[String]{
      override def serialize(element: String, aLong: lang.Long) = {
        println("3333333333333")
        val nObject: JSONObject = JSON.parseObject(element)
        new ProducerRecord[Array[Byte],Array[Byte]](nObject.getString("sink_topic"), null, element.getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    env.execute()
  }
}
