package com.msbjy.music.rt.scalacode.dim

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.msbjy.music.rt.scalacode.utils._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.kafka.common.serialization.StringDeserializer
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import java.sql.DriverManager
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import scala.collection.mutable.ListBuffer

/**
  *  将Kafka 中维度数据根据配置通过Phoenix写入到HBase 对应的配置表中
  *  数据处理思路如下：
  *  1.读取Kafka DWD层 “DWD_DIM_INFO_RT” topic数据
  *  2.使用flink Process方法对关注的维度表使用Phoenix SQL 创建表、对不同表不同列进行清洗、向维度表中插入数据
  */
object DimDataToHBase {
  private val consumeKafkaFromEarliest: Boolean = ConfigUtil.CONSUME_KAFKA_FORMEARLIEST
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val dwdDimInfoRtTopic: String = ConfigUtil.DWD_DIM_INFO_RT_TOPIC
  private val phoenixURL: String = ConfigUtil.PHOENIX_URL
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
    /**
      * 数据样例：
      * {"database":"rt_songdb","xid":872085,"data":{"star_level":0,"lyricist":"无信息","publish_to":"[2.0,8.0]",
      * "audio_format":0,"language":"C","source":1,"lyric_feature":0,"original_track":3,"duration":222,"subtitles_type":0,
      * "accompany_version":0,"video_feature":0,"bpm":0,"height":480,"accompany_quality":0,"product":"无信息","original_singer":"无信息",
      * "video_resolution":1,"album":"无信息","composer":"无信息","video_quality":1,"original_track_vol":55,"singing_type":0,
      * "video_format":"M","post_time":"2003-08-19 00:00:00","acc_track_vol":55,"Image_quality":0,"original_sound_quality":0,
      * "pinyin":"bingdilian","accompany_track":0,"singer_info":"[{\"name\":\"崔紫君\",\"id\":\"11150\"}]","authorized_company":"无信息",
      * "name":"并蒂莲","width":720,"other_name":"无信息","pinyin_first":"BDL","source_id":"C003593","video_make":0,"song_version":0,"status":1},
      * "phoenix_tbl_name":"DIM_SONG","xoffset":46443,"pk_col":"source_id","type":"delete","cols":"source_id,name,album,singer_info,
      * post_time,authorized_company","sink_topic":"DWD_DIM_INFO_RT","table":"song","ts":1620552198}
      */
      if(consumeKafkaFromEarliest){
        ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdDimInfoRtTopic,props).setStartFromEarliest())
      }else{
        ds = env.addSource(MyKafkaUtil.GetDataFromKafka(dwdDimInfoRtTopic,props))
      }

    ds.keyBy(line=>{
      JSON.parseObject(line).getString("phoenix_tbl_name")
    }).process(new KeyedProcessFunction[String,String,String] {

      //设置状态，存储每个Phoenix表是否被创建
      lazy private val valueState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("valueState",classOf[String]))

      var conn: Connection = _
      var pst: PreparedStatement = _

      //在open方法中，设置连接Phoenix ，方便后期创建对应的phoenix表
      override def open(parameters: Configuration): Unit = {
        println("创建Phoenix 连接... ...")
        conn = DriverManager.getConnection(phoenixURL)
      }

      override def processElement(jsonStr: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

        val nObject: JSONObject = JSON.parseObject(jsonStr)
        //从json 对象中获取对应 hbase 表名、主键、列信息
        val operateType: String = nObject.getString("type")
        val phoenixTblName: String = nObject.getString("phoenix_tbl_name")
        val pkCol: String = nObject.getString("pk_col")
        val cols: String = nObject.getString("cols")

        //判断操作类型，这里只会向HBase中存入增加、修改的数据，删除等其他操作不考虑
        //operateType.equals("bootstrap-insert") 这种情况主要是使用maxwell 直接批量同步维度数据时，操作类型为bootstrap-insert
        if(operateType.equals("insert")||operateType.equals("update")||operateType.equals("bootstrap-insert")){
          //判断状态中是否有当前表状态，如果有说明已经被创建，没有就组织建表语句，通过phoenix创建维度表
          if(valueState.value() ==null){
            createPhoenixTable(phoenixTblName, pkCol, cols)
            //更新状态
            valueState.update(phoenixTblName)
          }
          //向phoenix表中插入数据，同时方法中涉及数据清洗
          upsertIntoPhoenixTable(nObject, phoenixTblName, pkCol, cols)

          /**
            *  当有维度数据更新时，那么将Redis中维度表缓存删除
            *  Redis中 key 为：维度表-主键值
            */
          if(operateType.equals("update")){
            //获取当前更新数据中主键对应的值
            val pkValue: String = nObject.getJSONObject("data").getString(pkCol)
            //组织Redis中的key
            val key = phoenixTblName+"-"+pkValue
            //删除Redis中缓存的此key对应数据，没有此key也不会报错
            MyRedisUtil.deleteKey(key)
          }
          out.collect("执行成功")
        }
      }

      private def upsertIntoPhoenixTable(nObject: JSONObject, phoenixTblName: String, pkCol: String, cols: String): Unit = {
        //获取数据中"data"对应的json格式数据
        val currentInfoDataJson: JSONObject = nObject.getJSONObject("data")

        //获取向phoenix中插入数据所有列
        val colsList: ListBuffer[String] = MyStringUtil.getAllCols(cols)

        //针对不同的维度表，进行不同清洗
        //针对rt_songdb.song 表清洗 album,singer_info,post_time,authorized_company 四列即可
        if(phoenixTblName.equals("DIM_SONG")){
          //说明是 rt_songdb.song 表
          val album: String = currentInfoDataJson.getString("album")
          currentInfoDataJson.put("album",ETLUtil.getAlbumName(album))

          val singerInfo: String = currentInfoDataJson.getString("singer_info")
          currentInfoDataJson.put("singer_info",ETLUtil.getSingerName(singerInfo))

          val postTime: String = currentInfoDataJson.getString("post_time")
          currentInfoDataJson.put("post_time",DateUtil.formatDate(postTime))

          val authorizedCompany: String = currentInfoDataJson.getString("authorized_company")
          currentInfoDataJson.put("authorized_company",ETLUtil.getAuthCompanyName(authorizedCompany))

        }

        //针对rt_ycak.machine_local_info 表进行清洗，清洗 province、city字段
        if(phoenixTblName.equals("DIM_MACHINE_LOCAL_INFO")){
          //前期处理将空字段处理成了“无信息”
          if("无信息".equals(currentInfoDataJson.getString("province"))){
            val newProvince: String = ETLUtil.getProvince(currentInfoDataJson.getString("address"))
            currentInfoDataJson.put("province",newProvince)
          }

          if("无信息".equals(currentInfoDataJson.getString("city"))){
            val newCity: String = ETLUtil.getCity(currentInfoDataJson.getString("address"))
            currentInfoDataJson.put("city",newCity)
          }
        }

        //针对rt_ycak.machine_admin_map 表进行清洗，此表暂时没有需要清洗的字段，跳过
        if(phoenixTblName.equals("DIM_MACHINE_ADMIN_MAP")){

        }

        //获取主键对应的值
        val pkValue: String = currentInfoDataJson.getString(pkCol)

        //组织向表中插入数据的语句
        //upsert into test values ('1','zs',18);
        val upsertSQL = new StringBuffer(s"upsert into  ${phoenixTblName} values ('${pkValue}'")

        for (col <- colsList) {
          val currentColValue: String = currentInfoDataJson.getString(col)
          println("colsList = "+colsList.toString+" - current col = "+currentColValue)
          //将列数据中的 “'”符号进行转义
          upsertSQL.append(s",'${currentColValue.replace("'","\\'")}'")
        }
        upsertSQL.append(s")")

//        println("插入SQL语句 = " + upsertSQL.toString)

        //向表中Phoenix中插入数据
        println("Sql = "+upsertSQL.toString)
        pst = conn.prepareStatement(upsertSQL.toString)

        pst.execute()

        //这里如果想要批量提交，可以设置状态，当每个key 满足1000条时，commit一次，
        // 另外定义定时器，每隔2分钟自动提交一次，防止有些数据没有达到2000条时没有存入Phoenix
        conn.commit()
      }

      private def createPhoenixTable(phoenixTblName: String, pkCol: String, cols: String): Boolean = {
        //获取所有列
        val colsList: ListBuffer[String] = MyStringUtil.getAllCols(cols)

        //组织phoenix建表语句,为了后期操作方便，这里建表语句所有列族指定为“cf",所有字段都为varchar
        //create table xxx (id varchar primary key ,f1.name varchar,f1.age varchar);
        val createSql = new StringBuffer(s"create table if not exists ${phoenixTblName} (${pkCol} varchar primary key,")
        for (col <- colsList) {
          createSql.append(s"cf.${col.replace("'","\\'")} varchar,")
        }
        //将最后一个逗号替换成“) column_encoded_bytes=0” ，最后这个参数是不让phoenix对数据进行16进制编码
        createSql.replace(createSql.length() - 1, createSql.length(), ") column_encoded_bytes=0")

//        println(s"拼接Phoenix SQL 为 = ${createSql}")

        //执行sql
        pst = conn.prepareStatement(createSql.toString)
        pst.execute()
      }

      //关闭连接
      override def close(): Unit = {
        pst.close()
        conn.close()
      }

    }).print()

    env.execute()

  }
}
