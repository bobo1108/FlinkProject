package com.msbjy.music.rt.scalacode.bobo.dwd

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Create by Thinkpad on 2022/11/15.
  */
object TimeOpre {
  def main(args: Array[String]): Unit = {
    val action_time = 621360632477L
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val str: String = sdf.format(new Date(action_time))
    println(str)
  }
}
