package com.msbjy.music.rt.scalacode.mytestcode

import java.lang
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import scalaj.http.{Http, HttpOptions, HttpResponse}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.msbjy.music.rt.scalacode.utils.MyStringUtil
import org.apache.kafka.clients.producer.ProducerRecord

//处理DWD 层 "DWD_USER_LOGIN_LOCATION_INFO_RT"用户上报位置信息数据 ，调用高德api 拉宽数据 存入DWS层
object ProcessODSUserLocationToDWS {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.streaming.api.scala._
    val props = new Properties()
    props.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092")
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","group-1")
    val ds: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("DWD_USER_LOGIN_LOCATION_INFO_RT",
      new SimpleStringSchema(),props))

    //每10条数据调用一次高德api,获取地理位置信息
    val result: DataStream[String] = ds.countWindowAll(10).process(new ProcessAllWindowFunction[String, String, GlobalWindow] {
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {

        val list: List[String] = elements.toList
        //{"uid":"27690816","lng":"113.0054","login_dt":1627388941451,"mid":"89615","lat":"28.2002"}
        //创建拼接所有的经纬度的字符串
        val allLngLat = new StringBuilder()
        for (i <- 0 until list.size) {
          val nObject: JSONObject = JSON.parseObject(list(i))
          val lng: String = nObject.getString("lng")
          val lat: String = nObject.getString("lat")
          allLngLat.append(s"${lng},${lat}|")
        }

        //调用高德api
        val rps: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo")
          .param("key", "344bff6e68fdf2c56039a2bb8e4a36c6")
          .param("location", allLngLat.deleteCharAt(allLngLat.length - 1).toString())
          .param("batch", "true")
          .options(HttpOptions.connTimeout(5000))
          .asString
        println(rps.body)
        val rpsJson: JSONObject = JSON.parseObject(rps.body)
        if ("10000".equals(rpsJson.getString("infocode")) && rpsJson.getJSONArray("regeocodes").size() == list.size) {
          val jsonArry: JSONArray = rpsJson.getJSONArray("regeocodes")

          for (j <- 0 until jsonArry.size) {
            val currentJson: JSONObject = jsonArry.getJSONObject(j)
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

    //将拉宽的数据写入 Kafka DWS 层
    result.addSink(new FlinkKafkaProducer[String]("DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT",new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new  ProducerRecord[Array[Byte], Array[Byte]]("DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT",null,element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))



    env.execute()
  }
}
