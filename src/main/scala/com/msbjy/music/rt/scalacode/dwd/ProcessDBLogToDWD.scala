package com.msbjy.music.rt.scalacode.dwd

import java.util.Properties
import java.lang

import com.msbjy.music.rt.scalacode.utils.{ConfigUtil, MyKafkaUtil, MySQLUtil}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 针对 Kafka 中ODS层 “ODS_DB_BUSSINESS_DATA” 业务库数据进行清洗到DWD层
  *
  * 此代码主要作用是根据MySQL表 tbl_config_info 配置信息，将维度表和实时表分开存入对应的Kafka DWD层
  * 实现步骤思路如下：
  * 1.从Kafka读取ODS层 “ODS_DB_BUSSINESS_DATA” topic 数据
  * 2.从MySQL中读取配置表数据进行广播
  * 3.将两个流进行Connect连接，获取ODS层每条数据判断业务数据是否是 对应库-表-insert/update 数据，
  *   如果是则判断是维度表还是事实表，并将广播流中对应配置信息设置到此条数据中
  * 4.将事实数据或者维度数据存入Kafka DWD 层
  *
  * 注意先启动此程序，后启动maxwell向kafka中同步binlog业务数据，因为有广播状态流，启动顺序一定要注意
  */
object ProcessDBLogToDWD {
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val odsDbBussinessDataTopic: String = ConfigUtil.ODS_DB_BUSSINESS_DATA_TOPIC
  private val mysqlUrl: String = ConfigUtil.MYSQL_URL
  private val mysqlUser: String = ConfigUtil.MYSQL_USER
  private val mysqlPassWord: String = ConfigUtil.MYSQL_PASSWORD
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
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
    props.setProperty("group.id","ods_db_bussiness_data")

    //4.从数据中获取Kafka ODS层 数据
    if(consumeKafkaFromEarliest){
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsDbBussinessDataTopic,props).setStartFromEarliest())
    }else{
      ds = env.addSource(MyKafkaUtil.GetDataFromKafka(odsDbBussinessDataTopic,props))
    }

    //5.设置mapState,用于广播流
    val mapStateDescriptor = new MapStateDescriptor[String,JSONObject]("mapStateDescriptor",classOf[String],classOf[JSONObject])

    //6.从MySQL中获取配置信息，并广播
    val bcConfigDs: BroadcastStream[JSONObject] = env.addSource(MySQLUtil.getMySQLData(mysqlUrl,mysqlUser,mysqlPassWord)).broadcast(mapStateDescriptor)

    //7.设置维度数据侧输出流标记
    val dimDataTag = new OutputTag[String]("dim_data")

    //8.连接两个流进行处理
    val factMainDs: DataStream[String] = ds.connect(bcConfigDs).process(new BroadcastProcessFunction[String, JSONObject, String] {
      //处理正常事件流，从广播状态中获取数据，判断流事件是否是业务库关注数据，同时维度数据设置到侧输出流中,主流中存放事实数据
      override def processElement(value: String, ctx: BroadcastProcessFunction[String, JSONObject, String]#ReadOnlyContext, out: Collector[String]): Unit = {
        //获取广播状态
        val robcs: ReadOnlyBroadcastState[String, JSONObject] = ctx.getBroadcastState(mapStateDescriptor)

        //解析事件流数据
        val nObject: JSONObject = JSON.parseObject(value)
        //获取当前时间流来自的库和表 ，样例数据如下
        //{"database":"rt_songdb","table":"song","type":"insert","ts":1620559498,"xid":952413,"commit":true,"data":{xxxxx}}
        val dbName: String = nObject.getString("database")
        val tableName: String = nObject.getString("table")
        val key = dbName + ":" + tableName
        if (robcs.contains(key)) {
          val jsonValue: JSONObject = robcs.get(key)

          if (jsonValue.get("tbl_type").equals("dim")) {
            //维度数据，将对应的 jsonValue中的信息设置到流事件中
            nObject.put("pk_col", jsonValue.getString("pk_col"))
            nObject.put("cols", jsonValue.getString("cols"))
            nObject.put("phoenix_tbl_name", jsonValue.getString("phoenix_tbl_name"))
            nObject.put("sink_topic", jsonValue.getString("sink_topic"))
            ctx.output(dimDataTag, nObject.toString)
          } else {
            //事实数据,将对应的 jsonValue中的 sink_topic 信息设置到流事件中
            nObject.put("sink_topic", jsonValue.getString("sink_topic"))
            out.collect(nObject.toString)
          }
        }
      }

      //处理广播事件流
      override def processBroadcastElement(jsonObject: JSONObject, ctx: BroadcastProcessFunction[String, JSONObject, String]#Context, out: Collector[String]): Unit = {
        val tblDB: String = jsonObject.getString("tbl_db")
        val tblName: String = jsonObject.getString("tbl_name")
        //向状态中更新数据
        val bcs: BroadcastState[String, JSONObject] = ctx.getBroadcastState(mapStateDescriptor)
        bcs.put(tblDB + ":" + tblName, jsonObject)
        println("广播数据流设置完成...")
      }
    })

    //获取主流 事实数据 ,注意这里的 DWD_DEFAULT_TOPIC 不会存入数据，因为在内部已经设置了对应的 topic
    /**
      *  事实数据样例：
      * {"sink_topic":"DWD_MACHINE_CONSUMER_DETAIL_RT","database":"rt_ycak","table":"machine_consume_detail","type":"insert","ts":1620552242,
      * "commit":true,"data":{"id":17232,"mid":89779,"p_type":2,"m_type":0,"pkg_id":0,"pkg_name":"单曲","amount":300,
      * "consum_id":"无信息","order_id":"InsertCoin_14451_MID89779_2_1578196225","trade_no":"无信息",
      * "action_time":"20200105115025","uid":0,"nickname":"无信息","activity_id":0,"activity_name":"无信息",
      * "coupon_type":0,"coupon_type_name":"无信息","pkg_price":300,"pkg_discount":0,"order_type":1,"bill_date":"20200105"}}
      */
//    factMainDs.print("事实数据>>>>>>")
    factMainDs.addSink(new FlinkKafkaProducer[String]("DWD_DEFAULT_TOPIC",
      new KafkaSerializationSchema[String] {
        override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          val nObject: JSONObject = JSON.parseObject(element)
          //获取json 中的信息
          val topic: String = nObject.getString("sink_topic")
          val ts: String = nObject.getString("ts")

          //事实数据后期业务中有可能需要用到事件时间，这里将数据事件时间获取后设置到data json value 中，写出去
          nObject.getJSONObject("data").put("ts",ts)

          new ProducerRecord[Array[Byte],Array[Byte]](topic,null,nObject.getJSONObject("data").toString.getBytes())
        }
      },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))


    //获取侧输出流 维度数据 ,注意这里的default_topic 不会存入数据，因为在内部已经设置了对应的 topic
    /**
      *  维度数据样例：
      * {"sink_topic":"DWD_DIM_INFO_RT","database":"rt_songdb","table":"song","type":"insert","ts":1620552241,"xid":931649,"xoffset":1844,
      * "data":{"source_id":"C014287","name":"80之后","other_name":"无信息","source":1,"album":"无信息","product":"无信息",
      * "language":"C","video_format":"M","duration":202,"singer_info":"[{\"name\":\"钟舒漫\",\"id\":\"15375\"}]",
      * "post_time":"0.0","pinyin_first":"ZH","pinyin":"zhihou","singing_type":0,"original_singer":"无信息","lyricist":"无信息",
      * "composer":"无信息","bpm":0,"star_level":0,"video_quality":1,"video_make":0,"video_feature":0,"lyric_feature":0,"Image_quality":0,
      * "subtitles_type":0,"audio_format":0,"original_sound_quality":0,"original_track":1,"original_track_vol":45,"accompany_version":0,
      * "accompany_quality":0,"acc_track_vol":45,"accompany_track":0,"width":720,"height":480,"video_resolution":1,"song_version":0,
      * "authorized_company":"无信息","status":1,"publish_to":"[2.0]"}}
      */
//    factMainDs.getSideOutput(dimDataTag).print("维度数据>>>>>>")
    factMainDs.getSideOutput(dimDataTag).addSink(new FlinkKafkaProducer[String]("DWD_DIM_INFO_RT",
      new KafkaSerializationSchema[String] {
        override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          val nObject: JSONObject = JSON.parseObject(element)
          //获取json 中的信息
          val topic: String = nObject.getString("sink_topic")  //DWD_DIM_INFO_RT
          val db: String = nObject.getString("database")
          val tbl: String = nObject.getString("table")
          //组织写入Kafka topic 数据的key
          val topicKey = db+tbl

          //需要将整体的数据写出，不能只获取data
          new ProducerRecord[Array[Byte],Array[Byte]](topic,topicKey.getBytes(),nObject.toString.getBytes())
        }
      },props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE))

    env.execute()

  }
}
