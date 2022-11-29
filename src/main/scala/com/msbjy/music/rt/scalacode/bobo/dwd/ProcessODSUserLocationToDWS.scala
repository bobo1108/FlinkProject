package com.msbjy.music.rt.scalacode.bobo.dwd

import java.lang
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyStringUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import scalaj.http.{Http, HttpOptions, HttpResponse}

/**
  * 处理 DWD层用户上报位置信息，调用高的api拉宽数据
  */
object ProcessODSUserLocationToDWS {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group-1")

    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWD_USER_LOGIN_LOCATION_INFO_RT", new SimpleStringSchema(),props).setStartFromEarliest())

    //每10条数据调用一次高德api,获取地理位置信息
    val result: DataStream[String] = ds.countWindowAll(3).process(new ProcessAllWindowFunction[String, String, GlobalWindow] {
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
        //调用高德api
        val list: List[String] = elements.toList
        val allLngLat = new StringBuilder
        for (i <- 0 until list.size) {
          val nObject: JSONObject = JSON.parseObject(list(i))
          val lng: String = nObject.getString("lng")
          val lat: String = nObject.getString("lat")
          allLngLat.append(s"${lng},${lat}|")
        }

        val rps: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo")
          .param("key", "f49283c1a83f69121aa19090d918154b")
          .param("location", allLngLat.deleteCharAt(allLngLat.length - 1).toString())
          .param("batch", "true")
          .options(HttpOptions.connTimeout(5000))
          .asString
        val rpsJson: JSONObject = JSON.parseObject(rps.body)
        if ("10000".equals(rpsJson.getString("infocode")) && rpsJson.getJSONArray("regeocodes").size == list.size) {
          val jSONArray: JSONArray = rpsJson.getJSONArray("regeocodes")

          for (j <- 0 until jSONArray.size()) {
            val currentJson: JSONObject = jSONArray.getJSONObject(j)
            val province: String = MyStringUtil.checkString(currentJson.getJSONObject("addressComponent").getString("province"))
            val city: String = MyStringUtil.checkString(currentJson.getJSONObject("addressComponent").getString("city"))
            val district: String = MyStringUtil.checkString(currentJson.getJSONObject("addressComponent").getString("district"))
            val address: String = MyStringUtil.checkString(currentJson.getString("formatted_address"))

            val nObject: JSONObject = JSON.parseObject(list(j))
            nObject.put("province", province)
            nObject.put("city", city)
            nObject.put("district", district)
            nObject.put("address", address)
            out.collect(nObject.toString)
          }
        }
      }
    })

    result.addSink(new FlinkKafkaProducer[String]("DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT", new KafkaSerializationSchema[String] {
      override def serialize(element: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]]("DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT",null,element.getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
    result.print("aaaaabb")
    env.execute()
  }
}
