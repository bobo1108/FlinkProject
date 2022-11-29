package com.msbjy.music.rt.scalacode.utils

import java.io

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * ClickHouse工具类
  */
object MyClickHouseUtil {
  def insertIntoClickHouse(tableEnv:TableEnvironment,table:Table,tableSinkName:String,insertSQL:String,fields:Array[String],types:Array[TypeInformation[_]]): Unit ={

    //准备ClickHouse table sink
    val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
      .setDrivername(ConfigUtil.CLICKHOUSE_DRIVER)
      .setDBUrl(ConfigUtil.CLICKHOUSE_URL)
      .setUsername("default")
      .setPassword("")
      .setQuery(insertSQL)
      .setBatchSize(50) //设置批次量,默认5000条
      .setParameterTypes(types:_*) //_* ：在scala中将数组转换成不定长参数
      .build()

    //注册ClickHouse table Sink，设置sink 数据的字段及Schema信息
    tableEnv.registerTableSink(tableSinkName,sink.configure(fields,types))

    //将数据插入到 ClickHouse Sink 中
    tableEnv.insertInto(table,tableSinkName)

  }

}
