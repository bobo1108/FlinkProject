package com.msbjy.music.rt.scalacode.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * Phoenix 工具类
  */
object MyPhoenixUtil {

  var conn: Connection = DriverManager.getConnection(ConfigUtil.PHOENIX_URL)
  var pst: PreparedStatement = _
  var rs: ResultSet = _

//  def getSongDimInfoFromPhoenix(sql:String): JSONObject ={
//    pst = conn.prepareStatement(sql)
//    rs = pst.executeQuery()
//    var sourceId: String = "无信息"
//    var name: String = "无信息"
//    var album: String = "无信息"
//    var singerInfo: String = "无信息"
//    var postTime: String = "无信息"
//    var authorizedCompany: String = "无信息"
//    while (rs.next()) {
//      sourceId = rs.getString("source_id")
//      name = rs.getString("name")
//      album = rs.getString("album")
//      singerInfo = rs.getString("singer_info")
//      postTime = rs.getString("post_time")
//      authorizedCompany = rs.getString("authorized_company")
//    }
//
//    //组织json串，将此条数据设置到Redis中
//    val songDimJsonStr = new JSONObject()
//    songDimJsonStr.put("source_id", sourceId)
//    songDimJsonStr.put("name", name)
//    songDimJsonStr.put("album", album)
//    songDimJsonStr.put("singer_info", singerInfo)
//    songDimJsonStr.put("post_time", postTime)
//    songDimJsonStr.put("authorized_company", authorizedCompany)
//
//    songDimJsonStr
//  }

}
