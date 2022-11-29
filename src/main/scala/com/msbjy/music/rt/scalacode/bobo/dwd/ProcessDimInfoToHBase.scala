package com.msbjy.music.rt.scalacode.bobo.dwd

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.dim.DimDataToHBase.{consumeKafkaFromEarliest, ds, dwdDimInfoRtTopic}
import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, ETLUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 读取DWD_DIM_INFO_RT 数据通过Flink写入HBASE
  */
object ProcessDimInfoToHBase {
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  var dimDS: DataStream[String] = _
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx3")
    dimDS = env.addSource(MyKafkaUtil.GetDataFromKafka("DWD_DIM_INFO_RT",props))
//    if(consumeKafkaFromEarliest){
//      dimDS = env.addSource(MyKafkaUtil.GetDataFromKafka("DWD_DIM_INFO_RT",props).setStartFromEarliest())
//    }else{
//      dimDS = env.addSource(MyKafkaUtil.GetDataFromKafka("DWD_DIM_INFO_RT",props))
//    }
    //val dimDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWD_DIM_INFO_RT", new SimpleStringSchema(),props))


    val rst: DataStream[String] = dimDS.keyBy(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      nObject.getString("phoenix_table_name")

    }).process(new KeyedProcessFunction[String, String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _

      private lazy val phxTableState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("phxTableState", classOf[String]))

      override def open(parameters: Configuration): Unit = {
        println("连接phx...")
        conn = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
        println("已连接phx")
      }
      override def processElement(value: String, context: KeyedProcessFunction[String, String, String]#Context, collector: Collector[String]) = {
        val nObject: JSONObject = JSON.parseObject(value)
        //phx表
        val phxTable: String = nObject.getString("phoenix_table_name")
        val pkCol = nObject.getString("pk_col")
        var cols = nObject.getString("cols")

        val tp: String = nObject.getString("type")
        if ("insert".equals(tp) || "update".equals(tp) || "bootstrap-insert".equals(tp)) {
          //创建phx表
          var createSQL = new StringBuilder(s"create table if not exists ${phxTable} ( ${pkCol} varchar primary key , ")

          //拼接多个列
          for (col <- cols.split(",")) {
            createSQL.append(s"cf.${col} varchar ,")
          }

          createSQL.deleteCharAt(createSQL.length - 1).append(")")

          //println("建表语句是： " + createSQL.toString())

          pst = conn.prepareStatement(createSQL.toString())

          pst.execute()

          phxTableState.update(phxTable)

          //获取主键对应的value值
          val pkColValue: String = nObject.getString(pkCol)

          if ("update".equals(tp)) {
            //删除redis歌曲信息
            println("*** 删除Redis key : "+phxTable+"-"+pkColValue)
            MyRedisUtil.deleteKey(phxTable.toLowerCase()+"-"+pkColValue)
          }


          if ("DIM_SONG".equals(phxTable)) {
            nObject.put("singer_info",ETLUtil.getSingerName(nObject.getString("singer_info")))
            nObject.put("album",ETLUtil.getAlbumName(nObject.getString("album")))
            nObject.put("authorized_company",ETLUtil.getAuthCompanyName(nObject.getString("authorized_company")))
          }

          //插入数据
          val upsertSQL = new StringBuilder(s"upsert into ${phxTable} values ( '${pkColValue}' ,")

          for (col <- cols.split(",")) {
            val currentCloValue: String = nObject.getString(col)
            upsertSQL.append(s"'${currentCloValue.replace("'", "\\'")}',")
          }

          upsertSQL.deleteCharAt(upsertSQL.length - 1).append(")")
          println("插入数据语句：" + upsertSQL.toString())
          pst = conn.prepareStatement(upsertSQL.toString())
          pst.execute()
          conn.commit()
          //conn.setAutoCommit(true)
        }

      }
    })
    rst.print()
    env.execute()

  }
}
