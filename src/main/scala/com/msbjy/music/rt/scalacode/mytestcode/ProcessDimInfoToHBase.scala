package com.msbjy.music.rt.scalacode.mytestcode

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.{ETLUtil, MyRedisUtil}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

//读取 DWD层 - DWD_DIM_INFO_RT   数据通过phx 写入HBASE
object ProcessDimInfoToHBase {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    env.setParallelism(1)
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx3")
    val dimDS: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String]("DWD_DIM_INFO_RT",new SimpleStringSchema(),props))


    val result: DataStream[String] = dimDS.keyBy(line => {
      val nObject: JSONObject = JSON.parseObject(line)
      nObject.getString("phoenix_table_name")
    }).process(new KeyedProcessFunction[String, String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _

      //设置phx建表的状态
      private lazy val phxTableState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("phxTableState", classOf[String]))

      //在初始化ProcessFunction时，只执行一次
      override def open(parameters: Configuration): Unit = {
        println("连接phx...")
        conn = DriverManager.getConnection("jdbc:phoenix:node3,node4,hadoop102:2181")
        println("已连接phx")
      }

      //每条数据都会执行
      override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val nObject: JSONObject = JSON.parseObject(value)
        //phx表
        val phxTable: String = nObject.getString("phoenix_table_name")
        //获取主键
        val pkCol: String = nObject.getString("pk_col")
        //获取所有列
        val cols: String = nObject.getString("cols")

        //获取表类型, insert, update
        val tp: String = nObject.getString("type")

        if("insert".equals(tp)||"update".equals(tp)||"bootstrap-insert".equals(tp)){

          //判断状态中是否有表信息，如果有就不创建了，如果没有需要创建表
          if (phxTableState.value() == null) {
            //创建phx表 ,create table if not exists xx (col1 varchar primary key ,cf.clo2 varchar, cf.col3 varchar)
            var createSQL = new StringBuilder(s"create table if not exists ${phxTable} ( ${pkCol} varchar primary key ,")

            //拼接多个列
            for (col <- cols.split(",")) {
              createSQL.append(s" cf.${col} varchar ,")
            }

            createSQL.deleteCharAt(createSQL.length - 1).append(")")
            println("建表sql : " + createSQL.toString())

            pst = conn.prepareStatement(createSQL.toString())

            pst.execute()

            phxTableState.update(phxTable)
          }

          //获取主键对应的value值
          val pkColValue: String = nObject.getString(pkCol)

          if("update".equals(tp)){
            println("*** 删除Redis key : "+phxTable+"-"+pkColValue)
            //删除Redis中对应的 歌曲id信息
            MyRedisUtil.deleteKey(phxTable.toLowerCase()+"-"+pkColValue)
          }


          if("DIM_SONG".equals(phxTable)){
            //对数据进行清洗,歌手、专辑、发行公司
            nObject.put("singer_info",ETLUtil.getSingerName(nObject.getString("singer_info")))
            nObject.put("album",ETLUtil.getAlbumName(nObject.getString("album")))
            nObject.put("authorized_company",ETLUtil.getAuthCompanyName(nObject.getString("authorized_company")))
          }

          //向表中插入数据，upsert into xx valuse (pkcol-value,'col1','col2')
          val upsertSQL = new StringBuilder(s"upsert into ${phxTable} values ( '${pkColValue}' ,")

          for (col <- cols.split(",")) {
            //获取当前列对应的值
            val currentColValue: String = nObject.getString(col)
            upsertSQL.append(s"'${currentColValue.replace("'","\\'")}',")
          }

          upsertSQL.deleteCharAt(upsertSQL.length - 1).append(")")
          println("插入数据语句：" + upsertSQL.toString())

          pst = conn.prepareStatement(upsertSQL.toString())

          pst.execute()
          conn.commit()
//          conn.setAutoCommit(true)
        }

      }
    })

    result.print()

    env.execute()

  }

}
