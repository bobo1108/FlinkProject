package com.msbjy.music.rt.scalacode.dws

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, MyKafkaUtil, MyRedisUtil, MyStringUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}

/**
  *  针对 Kafka DWD层“DWD_MACHINE_CONSUMER_DETAIL_RT”订单数据，
  *  结合HBase中的维度数据，获取订单详细信息：机器的名称、投资人、承接方、公司、合作方各方的分成比例、机器省份、城市、详细地址信息
  *  将处理后的宽表数据写入DWS层 “DWS_ORDER_DETAIL_INFO_WIDE_RT”中。
  */
object ProcessMachineConsumerDetailInfoToDWS {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  private val dwdMachineConsumerDetailRtTopic: String = ConfigUtil.DWD_MACHINE_CONSUMER_DETAIL_RT_TOPIC
  private val hbaseDimMachineLocalInfoTbl: String = ConfigUtil.HBASE_DIM_MACHINE_LOCAL_INFO
  private val hbaseDimMachineAdminMapTbl: String = ConfigUtil.HBASE_DIM_MACHINE_ADMIN_MAP
  private val dwsMacheineConsumerDetailWideRt: String = ConfigUtil.DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT

  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //1.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //2.设置读取Kafka并行度
    env.setParallelism(3)

    //3.设置Kafka配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","dwd_machine_consumer_detail")

    //4.从数据中获取Kafka DWD层 数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdMachineConsumerDetailRtTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdMachineConsumerDetailRtTopic,props))
    }

    //5.针对获取的数据，进行关联维度处理
    val dwsDs: DataStream[String] = ds.process(new ProcessFunction[String, String] {
      var conn: Connection = _
      var pst: PreparedStatement = _
      var rs: ResultSet = _

      var i=0

      //开启Phoenix连接
      override def open(parameters: Configuration): Unit = {
        //连接Phoenix
        println(s"连接Phoenix ... ...")
        conn = DriverManager.getConnection(ConfigUtil.PHOENIX_URL)
      }

      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        println("i = "+i)
        i+=1
        //从事件流中获取数据，进行处理
        /**
          * {"activity_name":"无信息","amount":1000,"bill_date":"20210517","mid":97385,"coupon_type_name":"无信息",
          * "pkg_name":"15分钟包时","uid":0,"action_time":"1621253052324","consum_id":"无信息","nickname":"无信息",
          * "activity_id":0,"p_type":2,"trade_no":"无信息","pkg_price":1000,"id":38084,"pkg_id":1,"m_type":0,
          * "order_id":"InsertCoin_10269_MID97385_2_1578197593","pkg_discount":0,"order_type":1,"coupon_type":0,"ts":"1621253052"}
          */
        val nObject: JSONObject = JSON.parseObject(value)
        //获取机器id
        val mid: String = nObject.getString("mid")
        //根据机器mid 从Redis缓存中读取当前机器的省份城市信息
        val machineLocalRedisCacheInfo: String = MyRedisUtil.getInfoFromRedisCache(hbaseDimMachineLocalInfoTbl,mid)
        if (MyStringUtil.isEmpty(machineLocalRedisCacheInfo)) {
          //说明缓存中没有数据，从Phoenix中进行查询
          println("连接Phoenix查询机器位置维度数据")
          val sql = s"select mid,province_id,city_id,province,city,map_class,mglng,mglat,address,real_address,revenue_time,sale_time " +
            s"from dim_machine_local_info where mid = '${mid}'"

          pst = conn.prepareStatement(sql)
          rs = pst.executeQuery()

          var provinceId: String = "无信息"
          var cityId: String = "无信息"
          var province: String = "无信息"
          var city: String = "无信息"
          var mapClass: String = "无信息"
          var mglng: String = "无信息"
          var mglat: String = "无信息"
          var address: String = "无信息"
          var realAddress: String = "无信息"
          var revenueTime: String = "无信息"
          var saleTime: String = "无信息"

          while (rs.next()) {
            provinceId = rs.getString("province_id")
            cityId = rs.getString("city_id")
            province = rs.getString("province")
            city = rs.getString("city")
            mapClass = rs.getString("map_class")
            mglng = rs.getString("mglng")
            mglat = rs.getString("mglat")
            address = rs.getString("address")
            realAddress = rs.getString("real_address")
            revenueTime = rs.getString("revenue_time")
            saleTime = rs.getString("sale_time")
          }

          //组织json串，将此条数据设置到Redis中
          var machineLocalInfoDimJsonStr = new JSONObject()
          machineLocalInfoDimJsonStr.put("mid", mid)
          machineLocalInfoDimJsonStr.put("province_id", provinceId)
          machineLocalInfoDimJsonStr.put("city_id", cityId)
          machineLocalInfoDimJsonStr.put("province", province)
          machineLocalInfoDimJsonStr.put("city", city)
          machineLocalInfoDimJsonStr.put("map_class", mapClass)
          machineLocalInfoDimJsonStr.put("mglng", mglng)
          machineLocalInfoDimJsonStr.put("mglat", mglat)
          machineLocalInfoDimJsonStr.put("address", address)
          machineLocalInfoDimJsonStr.put("real_address", realAddress)
          machineLocalInfoDimJsonStr.put("revenue_time", revenueTime)
          machineLocalInfoDimJsonStr.put("sale_time", saleTime)

          //向Redis中设置数据缓存
          MyRedisUtil.setRedisDimCache(hbaseDimMachineLocalInfoTbl, mid, machineLocalInfoDimJsonStr.toString)

          //向当前事件流对应的json对象中设置 province和city信息
          nObject.put("province", machineLocalInfoDimJsonStr.getString("province"))
          nObject.put("city", machineLocalInfoDimJsonStr.getString("city"))

          //主动销毁对象 ，回收内存
          machineLocalInfoDimJsonStr = null
          System.gc()

        }else{
          //从Redis中获取到对应的机器位置信息,向当前事件流对应的json对象中设置 province和city信息
//          println("从Redis中查询机器位置维度数据")
          var machineLocalRedisCacheInfoJsonObj: JSONObject = JSON.parseObject(machineLocalRedisCacheInfo)
          nObject.put("province", machineLocalRedisCacheInfoJsonObj.getString("province"))
          nObject.put("city", machineLocalRedisCacheInfoJsonObj.getString("city"))

          //主动销毁对象 ，回收内存
          machineLocalRedisCacheInfoJsonObj = null
          System.gc()

        }

        //根据机器mid 从Redis缓存中读取当前机器与客户关系信息
        val machineAdminMapCacheInfo: String = MyRedisUtil.getInfoFromRedisCache(hbaseDimMachineAdminMapTbl,mid)
        if(MyStringUtil.isEmpty(machineAdminMapCacheInfo)){
          //说明Redis缓存中没有缓存，去Phoenix中进行查询
          println("连接Phoenix查询机器与客户关系维度数据")
          val sql = s"select machine_num,machine_name,package_name,inv_rate,age_rate,com_rate,par_rate,deposit,scene_address,ground_name " +
            s"from dim_machine_admin_map where machine_num = '${mid}'"

          pst = conn.prepareStatement(sql)
          rs = pst.executeQuery()

          var machineName: String = "无信息"
          var packageName: String = "无信息"
          var invRate: String = "100.0"
          var ageRate: String = "0.0"
          var comRate: String = "0.0"
          var parRate: String = "0.0"
          var deposit: String = "无信息"
          var sceneAddress: String = "无信息"
          var groundName: String = "无信息"

          while (rs.next()) {
            machineName = rs.getString("machine_name")
            packageName = rs.getString("package_name")
            invRate = rs.getString("inv_rate")
            ageRate = rs.getString("age_rate")
            comRate = rs.getString("com_rate")
            parRate = rs.getString("par_rate")
            deposit = rs.getString("deposit")
            sceneAddress = rs.getString("scene_address")
            groundName = rs.getString("ground_name")
          }

          //组织json串，将此条数据设置到Redis中
          var machineAdminMapInfoDimJsonStr = new JSONObject()
          machineAdminMapInfoDimJsonStr.put("mid", mid)
          machineAdminMapInfoDimJsonStr.put("machine_name", machineName)
          machineAdminMapInfoDimJsonStr.put("package_name", packageName)
          machineAdminMapInfoDimJsonStr.put("inv_rate", invRate)
          machineAdminMapInfoDimJsonStr.put("age_rate", ageRate)
          machineAdminMapInfoDimJsonStr.put("com_rate", comRate)
          machineAdminMapInfoDimJsonStr.put("par_rate", parRate)
          machineAdminMapInfoDimJsonStr.put("deposit", deposit)
          machineAdminMapInfoDimJsonStr.put("scene_address", sceneAddress)
          machineAdminMapInfoDimJsonStr.put("ground_name", groundName)

          //向Redis中设置数据缓存
          MyRedisUtil.setRedisDimCache(hbaseDimMachineAdminMapTbl, mid, machineAdminMapInfoDimJsonStr.toString)

          //向当前事件流对应的json对象中设置机器名称、机器套餐、投资方、承接方、合作方、公司四方分成信息、场景地址、场地名称
          nObject.put("machine_name", machineAdminMapInfoDimJsonStr.getString("machine_name"))
          nObject.put("package_name", machineAdminMapInfoDimJsonStr.getString("package_name"))
          nObject.put("inv_rate", machineAdminMapInfoDimJsonStr.getString("inv_rate"))
          nObject.put("age_rate", machineAdminMapInfoDimJsonStr.getString("age_rate"))
          nObject.put("com_rate", machineAdminMapInfoDimJsonStr.getString("com_rate"))
          nObject.put("par_rate", machineAdminMapInfoDimJsonStr.getString("par_rate"))
          nObject.put("scene_address", machineAdminMapInfoDimJsonStr.getString("scene_address"))
          nObject.put("ground_name", machineAdminMapInfoDimJsonStr.getString("ground_name"))

          //主动销毁对象 ，回收内存
          machineAdminMapInfoDimJsonStr = null
          System.gc()
        }else{
          //从Redis中获取到对应的机器客户关系信息
          // 向当前事件流对应的json对象中设置机器名称、机器套餐、投资方、承接方、合作方、公司四方分成信息、场景地址、场地名称
//          println("从Redis中查询机器与客户关系维度数据")
          var machineAdminMapCacheInfoJsonObject: JSONObject = JSON.parseObject(machineAdminMapCacheInfo)
          nObject.put("machine_name", machineAdminMapCacheInfoJsonObject.getString("machine_name"))
          nObject.put("package_name", machineAdminMapCacheInfoJsonObject.getString("package_name"))
          nObject.put("inv_rate", machineAdminMapCacheInfoJsonObject.getString("inv_rate"))
          nObject.put("age_rate", machineAdminMapCacheInfoJsonObject.getString("age_rate"))
          nObject.put("com_rate", machineAdminMapCacheInfoJsonObject.getString("com_rate"))
          nObject.put("par_rate", machineAdminMapCacheInfoJsonObject.getString("par_rate"))
          nObject.put("scene_address", machineAdminMapCacheInfoJsonObject.getString("scene_address"))

          //主动销毁对象 ，回收内存
          machineAdminMapCacheInfoJsonObject = null
          System.gc()
        }

        //返回数据
        out.collect(nObject.toString)
      }

      override def close(): Unit = {
        rs.close()
        pst.close()
        conn.close()
      }
    })


    //6.将结果写入Kafka DWS层“DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT”topic中
    dwsDs.addSink(MyKafkaUtil.WriteDataToKafkaWithOutKey(dwsMacheineConsumerDetailWideRt,props))

    env.execute()

  }

}
