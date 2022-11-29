package com.msbjy.music.rt.scalacode.utils

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.Date

/**
  *  时间工具类
  */
object DateUtil {
  /**
    * 将字符串格式化成"yyyy-MM-dd HH:mm:ss"格式
    * @param stringDate
    * @return
    */
  def formatDate(stringDate:String):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var formatDate = ""
    try{
      formatDate = sdf.format(sdf.parse(stringDate))
    }catch{
      case e:Exception=>{
        try{
          val bigDecimal = new BigDecimal(stringDate)
          val date = new Date(bigDecimal.longValue())
          formatDate = sdf.format(date)
        }catch{
          case e:Exception=>{
            formatDate
          }
        }
      }
    }
    formatDate
  }


  /**
    *  根据传递过来的时间戳，转换成"yyyy-MM-dd"
    */
  def getDateYYYYMMDD(tm:Long) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(new Date(tm))
  }

  /**
    *  根据传递过来的时间戳，转换成"yyyy-MM-dd"
    */
  def getDateYYYYMMDDHHMMSS(tm:Long) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(new Date(tm))
  }
}
