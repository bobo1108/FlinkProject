package com.msbjy.music.rt.scalacode.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * 读取MySQL数据工具类
  */
object MySQLUtil {
  //从MySQL中获取数据
  def getMySQLData(mysqlUrl:String,mysqlUser:String,mysqlPassWord:String) = {
    val rsf = new RichSourceFunction[JSONObject] {
      var flag = true
      var conn: Connection = _
      var pst: PreparedStatement = _
      var rs: ResultSet = _

      //连接mysql 创建连接
      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection(mysqlUrl,mysqlUser,mysqlPassWord)
        pst = conn.prepareStatement("select tbl_name,tbl_db,tbl_type,pk_col,cols,phoenix_tbl_name,sink_topic from rt_songdb.tbl_config_info")
      }

      override def run(ctx: SourceFunction.SourceContext[JSONObject]): Unit = {
        while (flag) {
          rs = pst.executeQuery
          while (rs.next()) {
            val tblName: String = rs.getString("tbl_name")
            val tblDB: String = rs.getString("tbl_db")
            val tblType: String = rs.getString("tbl_type")
            val pkCol: String = rs.getString("pk_col")
            val cols: String = rs.getString("cols")
            val phoenixTblName: String = rs.getString("phoenix_tbl_name")
            val sinkTopic: String = rs.getString("sink_topic")
            //组织json格式数据
            val jsonObject = new JSONObject()
            jsonObject.put("tbl_name", tblName)
            jsonObject.put("tbl_db", tblDB)
            jsonObject.put("tbl_type", tblType)
            jsonObject.put("pk_col", pkCol)
            jsonObject.put("cols", cols)
            jsonObject.put("phoenix_tbl_name", phoenixTblName)
            jsonObject.put("sink_topic", sinkTopic)
            ctx.collect(jsonObject)
          }
          rs.close()
          Thread.sleep(5 * 60 * 1000) //5分钟重复读取一次 mysql中对应表数据，这样mysql中更新的数据也能读取到
        }
      }

      override def close(): Unit = {
        pst.close()
        conn.close()

      }

      override def cancel(): Unit = {
        flag = false
      }

    }
    rsf
  }
}
