package com.msbjy.music.rt.scalacode.utils

import scala.collection.mutable.ListBuffer

/**
  *  字符串工具类
  */
object MyStringUtil {
  //字符串含有多个列，根据逗号隔开，此方法将所有列按逗号切割放入集合返回
  def getAllCols(colStr:String) = {
    val cols: Array[String] = colStr.split(",")
    val returnListBuffer = new ListBuffer[String]()
    for (col <- cols) {
      returnListBuffer.append(col)
    }
    returnListBuffer
  }

  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str: String): Boolean = {
    str == null || "".equals(str)
  }


  def strToInt(str:String) :Int = {
    if(isEmpty(str)){
      0
    }else{
      str.toInt
    }
  }

  /**
    * 检查字符串是否为空
    * @param str
    * @return
    */
  def checkString(str:String) = {
    if(str == null || "".equals(str) || "[]".equals(str)) "无信息" else str
  }

}
