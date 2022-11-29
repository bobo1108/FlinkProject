package com.msbjy.music.rt.scalacode.bobo.dwd

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyRedisUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 读取DWD 层 数据 与 DIM 层数据关联 ，将数据拉宽，写入DWS层
  */
object ProcessOrderInfoToDWS {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._

    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","xx2")
    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWD_MACHINE_CONSUMER_DETAIL_RT",
      new SimpleStringSchema(),props).setStartFromEarliest())

    //ds.print()
    //关联DIM 层
    val ds2: DataStream[String] = ds.process(new ProcessFunction[String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
      }

      override def processElement(value: String, context: ProcessFunction[String, String]#Context, out: Collector[String]) = {
        val nObject: JSONObject = JSON.parseObject(value)
        val mid: String = nObject.getString("mid")

        val redisJson: String = MyRedisUtil.getInfoFromRedisCache("DIM_MACHINE_LOCAL_INFO", mid)
        if (redisJson != null) {
          println("从redis中获取及其位置信息")
          val redisObject: JSONObject = JSON.parseObject(redisJson)
          nObject.put("province", redisObject.getString("province"))
          nObject.put("city", redisObject.getString("city"))
          nObject.put("address", redisObject.getString("address"))
        } else {
          println("从 phx 中获取机器位置信息")
          pst = conn.prepareStatement(s"select province, city, address from DIM_MACHINE_LOCAL_INFO where MID='${mid}'")
          val rst: ResultSet = pst.executeQuery()
          while (rst.next()) {
            val province: String = rst.getString("province")
            val city: String = rst.getString("city")
            val address: String = rst.getString("address")

            nObject.put("province", province)
            nObject.put("city", city)
            nObject.put("address", address)

            //想redis写缓存
            MyRedisUtil.setRedisDimCache("DIM_MACHINE_LOCAL_INFO", mid,
              s"""
                 |{"province":"${province}","city":"${city}","address":"${address}"}
            """.stripMargin)
          }
        }

        //获取 机器的 四方（投资人、合伙人、运营人、公司）分成比例
        val redisAdminJson: String = MyRedisUtil.getInfoFromRedisCache("DIM_MACHINE_ADMIN_MAP", mid)
        if (redisAdminJson != null) {
          println("从Redis中获取四方分成比例")
          val redisAdmin: JSONObject = JSON.parseObject(redisAdminJson)
          nObject.put("machine_name", redisAdmin.getString("machine_name"))
          nObject.put("inv_rate", redisAdmin.getString("inv_rate"))
          nObject.put("age_rate", redisAdmin.getString("age_rate"))
          nObject.put("com_rate", redisAdmin.getString("com_rate"))
          nObject.put("par_rate", redisAdmin.getString("par_rate"))
        } else {
          println("从 phx 中获取四方分成比例")
          pst = conn.prepareStatement(s"select machine_name,inv_rate,age_rate,com_rate,par_rate from DIM_MACHINE_ADMIN_MAP where MACHINE_NUM = '${mid}'")
          val rst: ResultSet = pst.executeQuery()
          while (rst.next()) {
            val machine_name: String = rst.getString("machine_name")
            val inv_rate: String = rst.getString("inv_rate")
            val age_rate: String = rst.getString("age_rate")
            val com_rate: String = rst.getString("com_rate")
            val par_rate: String = rst.getString("par_rate")

            nObject.put("machine_name", machine_name)
            nObject.put("inv_rate", inv_rate)
            nObject.put("age_rate", age_rate)
            nObject.put("com_rate", com_rate)
            nObject.put("par_rate", par_rate)

            //向redis设置缓存
            MyRedisUtil.setRedisDimCache("DIM_MACHINE_ADMIN_MAP", mid,
              s"""
                 |{"machine_name":"${machine_name}","inv_rate":"${inv_rate}","age_rate":"${age_rate}","com_rate":"${com_rate}","par_rate":"${par_rate}"}
              """.stripMargin)
          }
        }

        if (nObject.getString("province") != null && nObject.getString("inv_rate") != null) {
          out.collect(nObject.toString)
        }
      }

      override def close(): Unit = {
        conn.close()
        pst.close()
      }
    })

    ds2.addSink(new FlinkKafkaProducer[String]("DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT", new KafkaSerializationSchema[String] {
      override def serialize(ele: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]]("DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT", null, ele.getBytes())
      }
    },props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
    env.execute()
  }
}
