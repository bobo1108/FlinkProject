package com.msbjy.music.rt.scalacode.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {
  /**
    *  ConfigFactory.load() 默认加载classpath下的application.conf,application.json和application.properties文件。
    *
    */
  lazy val load: Config = ConfigFactory.load()
  val CONSUME_KAFKA_FORMEARLIEST = load.getBoolean("kafka.consumerdata.fromearliest")

  val KAFKA_BROKERS = load.getString("kafka.cluster")

  val ODS_DB_BUSSINESS_DATA_TOPIC = load.getString("kafka.db.topic")
  val ODS_LOG_USERLOG_TOPIC = load.getString("kafka.userlog.topic")


  val DWD_SONG_PLAY_INFO_RT_TOPIC = load.getString("kafka.userplaysong.topic")
  val DWD_USER_LOGIN_LOCATION_INFO_RT_TOPIC = load.getString("kafka.userloginlocationinfo.topic")
  val DWD_OTHER_USERLOG_RT_TOPIC = load.getString("kafka.otheruserlog.topic")
  val DWD_DIM_INFO_RT_TOPIC = load.getString("kafka.dim.topic")
  val DWD_MACHINE_CONSUMER_DETAIL_RT_TOPIC = load.getString("kafka.machine.consumer.detail.rt.topic")


  val DWS_SONG_PLAY_INFO_WIDE_RT_TOPIC = load.getString("kafka.dws.userplaysong.wide.topic")
  val DWS_USER_LOGIN_LOCATION_INFO_RT_TOPIC = load.getString("kafka.dws.userloginlocationinfo.wide.topic")
  val DWS_MACHINE_CONSUMER_DETAIL_WIDE_RT = load.getString("kafka.dws.machine.consumer.detail.wide.topic")

  val HBASE_DIM_SONG = load.getString("hbase.dim.song")
  val HBASE_DIM_MACHINE_ADMIN_MAP = load.getString("hbase.dim.machine.admin.map")
  val HBASE_DIM_MACHINE_LOCAL_INFO = load.getString("hbase.dim.machine.local.info")

  val PHOENIX_URL = load.getString("phoenix.url")

  val MYSQL_URL = load.getString("mysql.url")
  val MYSQL_USER = load.getString("mysql.user")
  val MYSQL_PASSWORD = load.getString("mysql.password")

  val REDIS_HOST = load.getString("redis.host")
  val REDIS_PORT = load.getString("redis.port")
  val REDIS_DB = load.getString("redis.db")

  val CLICKHOUSE_DRIVER = load.getString("clickhouse.driver")
  val CLICKHOUSE_URL = load.getString("clickhouse.url")

  val CHTBL_DM_SONGPLAY_HOT = load.getString("clickhouse.table.songplayhot")
  val CHTBL_DM_USER_LOGIN_LOC_INFO_RT = load.getString("clickhouse.table.userloginlocinfort")
  val CHTBL_DM_ORDER_INFO_RT = load.getString("clickhouse.table.orderinfort")
}
