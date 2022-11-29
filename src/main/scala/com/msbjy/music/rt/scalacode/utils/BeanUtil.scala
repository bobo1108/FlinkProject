package com.msbjy.music.rt.scalacode.utils

/**
  *   {
  *     "optrate_type": 0,
  *     "dur_time": 0,
  *     "mid": 40933,
  *     "session_id": 1031,
  *     "songname": "年少有为",
  *     "play_time": 0,
  *     "uid": 0,
  *     "singer_info": "李荣浩",
  *     "consume_type": 0,
  *     "time": 1620985690304,
  *     "pkg_id": 100,
  *     "songid": "lx149965",
  *     "order_id": ""
  *   }
  */
//设置用户点播歌曲日志对象
case class PlaySongInfo(operateType:String,durTime:Long,mid:String,sessionId:String,songName:String,
                        playTime:Long,uid:String,singerInfo:String,consumerType:String,time:Long,pkgId:String,songId:String,orderId:String)

//设置歌曲歌手热度对象
//song_id,song_name,singer_name,song_playtimes,singer_playtimes,data_dt,windowStart,windowEnd
case class SongAndSingerHotInfo(songId:String,songName:String,singerName:String,songPlayTimes:Long,singerPlayTimes:Long,dataDt:String,windowStart:String,windowEnd:String)

//设置用户登录位置信息对象
case class UserLoginInfo(uid:String,mid:String,province:String,city:String,district:String,address:String,lng:String,lat:String,loginDt:Long)

//设置用户登录位置信息统计对象
case class LoginLocTimesInfo(province:String,city:String,district:String,lng:String,lat:String,loginTimes:Long,loginDt:String)


//机器订单详细信息
case class MachineOrderDetail(province:String,city:String,groundName:String,sceneAddress:String,mid:String,machineName:String,
                              pkgName:String,amount:Double,invRate:Double,ageRate:Double,comRate:Double,parRate:Double,billDate:String,actionTime:Long)

//机器营收信息
case class MachineRevDetail(province:String,city:String,address:String,mid:String,machineName:String,pkgName:String,
                            amount:Double,invRev:Double,ageRev:Double,comRev:Double,parRev:Double,dataDt:String)

object BeanUtil {

}
