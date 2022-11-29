package com.msbjy.music.rt.scalacode.mytestcode

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.producer.ProducerRecord



case class TblConfig(tblName:String,tblDB:String,tblType:String,pkCol:String,cols:String,phoenixTableName:String,sinkTopic:String)


//读取Kafka ODS ODS_DB_BUSSINESS_DATA --业务库所有表数据 根据配置来决定数据写往Kafka DWD 哪个topic
object ProcessODSDBToDWD {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx2")
    //各种业务库表数据 - fact+dim
    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("ODS_DB_BUSSINESS_DATA",new SimpleStringSchema(),props))

    //Flink读取MySQL 数据，做广播流
    val configDS: DataStream[TblConfig] = env.addSource(new RichSourceFunction[TblConfig] {
      var conn: Connection = _
      var pst: PreparedStatement = _
      var flag = true

      override def open(parameters: Configuration): Unit = {
        //加载驱动
        conn = DriverManager.getConnection("jdbc:mysql://hadoop104:3306/rt_songdb", "root", "123")
      }

      override def run(ctx: SourceFunction.SourceContext[TblConfig]): Unit = {
        while (flag) {
          pst = conn.prepareStatement("select  tbl_name,tbl_db,tbl_type,pk_col,cols,phoenix_tbl_name,sink_topic from tbl_config_info")
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

      override def cancel(): Unit = {
        flag = false
      }

      override def close(): Unit = {
        pst.close()
        conn.close()
      }

    })

    //广播mysql流
    val msd = new MapStateDescriptor[String,TblConfig]("msd",classOf[String],classOf[TblConfig])
    val bcConfigDS: BroadcastStream[TblConfig] = configDS.broadcast(msd)


    val resultDs: DataStream[String] = ds.connect(bcConfigDS).process(new BroadcastProcessFunction[String, TblConfig, String] {
      //处理主流ds数据
      override def processElement(value: String, ctx: BroadcastProcessFunction[String, TblConfig, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        val bcDS: ReadOnlyBroadcastState[String, TblConfig] = ctx.getBroadcastState(msd)
        //根据广播流，判断数据应该写往哪个topic
        val nObject: JSONObject = JSON.parseObject(value)
        //获取库名称
        val db: String = nObject.getString("database")
        val tableName: String = nObject.getString("table")
        val tp: String = nObject.getString("type")
        val ts: String = nObject.getString("ts")

        println(value)
        if (bcDS.contains(db + ":" + tableName)) {
          //能进来说明就是我们关注业务库和表
          val config: TblConfig = bcDS.get(db + ":" + tableName)
          val dataJson = nObject.getJSONObject("data")
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
      override def processBroadcastElement(value: TblConfig, ctx: BroadcastProcessFunction[String, TblConfig, String]#Context, out: Collector[String]): Unit = {
        println("设置广播流。。。。")
        val bs: BroadcastState[String, TblConfig] = ctx.getBroadcastState(msd)
        //获取库名+“:”+表名
        bs.put(value.tblDB + ":" + value.tblName, value)
      }
    })



    //写出 到 DWD 对应的kafka topic中
    resultDs.addSink(new FlinkKafkaProducer[String]("xxx",new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val nObject: JSONObject = JSON.parseObject(element)

        new  ProducerRecord[Array[Byte], Array[Byte]](nObject.getString("sink_topic"),null,element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    env.execute()

  }
}
