package com.msbjy.music.rt.scalacode.dws

import java.util.Properties

import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, MyKafkaUtil, MyStringUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  *  处理用户登录信息
  *  将Kafka DWD层中的 "DWD_USER_LOGIN_LOCATION_INFO_RT" topic 通过访问高德Api
  *  将获取的地址信息存入Kafka DWS层 “DWS_USER_LOGIN_LOCATION_INFO_WIDE_RT” topic
  */
object ProcessUserLoginDataToDWS {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  private val dwdUserLoginLocationInfoRtTopic: String = ConfigUtil.DWD_USER_LOGIN_LOCATION_INFO_RT_TOPIC
  private val dwsUserLoginLocationInfoRtTopic: String = ConfigUtil.DWS_USER_LOGIN_LOCATION_INFO_RT_TOPIC

  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //2.设置配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","ods_userlog_group")

    //3.从数据中指定数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdUserLoginLocationInfoRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdUserLoginLocationInfoRtTopic,props))
    }

    //4.设置窗口对每10条数据进行调用高德api，获取用户登录的详细位置信息
    val endDS: DataStream[String] = ds.countWindowAll(10).process(new ProcessAllWindowFunction[String, String, GlobalWindow] {

      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
        val list: List[String] = elements.iterator.toList
        var concatLngLatStr = ""
        for (elem <- list) {
          val nObject: JSONObject = JSON.parseObject(elem)
          val lng: String = nObject.getString("lng")
          val lat: String = nObject.getString("lat")
          concatLngLatStr += lng + "," + lat + "|"
        }

        val response: HttpResponse[String] = Http("https://restapi.amap.com/v3/geocode/regeo")
          .param("key", "f49283c1a83f69121aa19090d918154b")
          .param("location", concatLngLatStr.substring(0, concatLngLatStr.length - 1))
          .param("batch", "true")
          .option(HttpOptions.readTimeout(10000)) //获取数据延迟 10s
          .asString
        println("本次数据 --- "+list.toString())
        val jsonInfo: JSONObject = JSON.parseObject(response.body.toString)
        if (jsonInfo == null) {
          println(response.body.toString)
        }
        val returnLocLength = JSON.parseArray(jsonInfo.getString("regeocodes")).size() //结果中返回的地址个数
        if ("10000".equals(jsonInfo.getString("infocode")) && list.size == returnLocLength) {
          //如果 info 返回1000 代表请求成功，并返回了结果
          //从返回的json中获取详细地址，对从高德API中查询的数据进行整理，转换成Row类型的数据返回
          val jsonArray: JSONArray = JSON.parseArray(jsonInfo.getString("regeocodes"))
          for (i <- 0 until returnLocLength) {
            val currentJsonObject = jsonArray.getJSONObject(i)
            //获取省份
            val province = MyStringUtil.checkString(currentJsonObject.getJSONObject("addressComponent").getString("province"))

            //获取城市
            val city = MyStringUtil.checkString(currentJsonObject.getJSONObject("addressComponent").getString("city"))

            //获取区县
            val district = MyStringUtil.checkString(currentJsonObject.getJSONObject("addressComponent").getString("district"))

            //获取详细地址信息
            val address = MyStringUtil.checkString(currentJsonObject.getString("formatted_address"))

            val nObject: JSONObject = JSON.parseObject(list(i))
            nObject.put("province", province)
            nObject.put("city", city)
            nObject.put("district", district)
            nObject.put("address", address)
            out.collect(nObject.toString)
          }
        }
      }
    })

    //保存到Kafka中
    endDS.addSink(MyKafkaUtil.WriteDataToKafkaWithOutKey(dwsUserLoginLocationInfoRtTopic,props))

    env.execute()
  }
}
