package com.msbjy.music.rt.scalacode.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  *  清洗数据ETL 工具类
  */
object ETLUtil {
  //根据地址详细信息获取城市
  def getCity(addr:String) = {
    var returnCity = "无信息"
    if(addr.contains("内蒙古自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("宁夏回族自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("广西壮族自治区")){
      returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
    }else if(addr.contains("新疆维吾尔自治区")){
      try{
        returnCity = addr.substring(addr.indexOf("区")+1,addr.indexOf("市"))
      }catch{
        case e:Exception =>{
          val index = addr.indexOf("区")
          if(addr.substring(index+1,addr.length).contains("区")){
            returnCity = addr.substring(index+1,addr.indexOf("区",index+1))
          }else{
            returnCity = addr.substring(index+1,addr.indexOf("州",index+1))
          }
        }
      }
    }else if(addr.contains("北京市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("上海市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("重庆市")){
      val index = addr.indexOf("市")
      if(addr.substring(index+1,addr.length).contains("区")){
        returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
      }else if(addr.substring(index+1,addr.length).contains("县")){
        returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("县"))
      }

    }else if(addr.contains("天津市")){
      returnCity = addr.substring(addr.indexOf("市")+1,addr.indexOf("区"))
    }else if(addr.contains("省")){
      val index = addr.indexOf("省")
      if(addr.substring(index+1,addr.length).contains("市")){
        returnCity = addr.substring(index+1,addr.indexOf("市"))
      }else if(addr.substring(index+1,addr.length).contains("自治州")){
        returnCity = addr.substring(index+1,addr.substring(index+1,addr.length).indexOf("州"))
      }
      returnCity
    }
    returnCity
  }


  //根据地址详细信息获取省份
  def getProvince(addr:String):String = {
    var returnProvince = "无信息"
    if(addr.contains("内蒙古自治区")){
      returnProvince ="内蒙古"
    }else if(addr.contains("宁夏回族自治区")){
      returnProvince ="宁夏"
    }else if(addr.contains("西藏自治区")){
      returnProvince ="西藏"
    }else if(addr.contains("广西壮族自治区")){
      returnProvince ="广西"
    }else if(addr.contains("新疆维吾尔自治区")){
      returnProvince ="新疆"
    }else if(addr.contains("北京市")){
      returnProvince ="北京"
    }else if(addr.contains("上海市")){
      returnProvince ="上海"
    }else if(addr.contains("重庆市")){
      returnProvince ="重庆"
    }else if(addr.contains("天津市")){
      returnProvince ="天津"
    }else if(addr.contains("省")){
      returnProvince = addr.substring(0,addr.indexOf("省"))
    }
    returnProvince
  }

  //获取授权公司信息
  def getAuthCompanyName(authCompanyInfo: String): Any = {
    var authCompanyName = "乐心曲库"
    try{
      val jsonObject: JSONObject = JSON.parseObject(authCompanyInfo)
      authCompanyName = jsonObject.getString("name")
    }catch{
      case e:Exception=>{
        authCompanyName
      }
    }
    authCompanyName
  }

  //根据歌手json 信息获取歌手信息，这里只获取第一个歌手名称
  def getSingerName(singerInfos: String): String = {
    var singerName = "未知歌手"
    try{
      val jsonArray: JSONArray = JSON.parseArray(singerInfos)
      singerName = jsonArray.getJSONObject(0).getString("name")
    }catch{
      case e:Exception =>{
        singerName
      }
    }
    singerName
  }


  //根据专辑信息解析json,获取专辑
  def getAlbumName(albumInfo: String): String = {
    var albumName = ""
    try{
      val jsonArray: JSONArray = JSON.parseArray(albumInfo)
      albumName = jsonArray.getJSONObject(0).getString("name")
    }catch{
      case e:Exception =>{
        if(albumInfo.contains("《")&&albumInfo.contains("》")){
          albumName = albumInfo.substring(albumInfo.indexOf('《'),albumInfo.indexOf('》')+1)
        }else{
          albumName = "暂无专辑"
        }
      }
    }
    albumName
  }




}
