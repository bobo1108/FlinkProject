package com.msbjy.music.rt.scalacode.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Redis 工具类
  */
object MyRedisUtil {

  val redisHost = ConfigUtil.REDIS_HOST
  val redisPort = MyStringUtil.strToInt(ConfigUtil.REDIS_PORT)
  val redisDB = MyStringUtil.strToInt(ConfigUtil.REDIS_DB)

  val redisTimeout = 30000

  /**
    * JedisPool是一个连接池，既可以保证线程安全，又可以保证了较高的效率。
    */
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)


  //根据给定的key ,去Redis中查询对应的Value数据
  //Redis中数据格式为set ：（维度表名-维度key,维度jsonstr），这里set格式后期方便设置key的过期时间，hset api不支持过期时间
  def getInfoFromRedisCache(hbaseDimTbl:String,key: String): String = {
    val jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)
    val jsonStr: String = jedis.get(hbaseDimTbl+"-"+key)
    MyRedisUtil.pool.returnResource(jedis)
    jsonStr
  }


  //向Redis 对应的维度表中设置
  def setRedisDimCache(hbaseDimTbl: String, key: String,jsonStr: String): Unit = {
    val jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)
    //设置过期时间 ,为24小时过期
//    jedis.setex(hbaseDimTbl+"-"+key,60,jsonStr)
    jedis.setex(hbaseDimTbl+"-"+key,60*60*24,jsonStr)
    MyRedisUtil.pool.returnResource(jedis)
  }

  //Redis中删除key
  def deleteKey(key: String) = {
    val jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)
    jedis.del(key)
    MyRedisUtil.pool.returnResource(jedis)
  }


  def main(args: Array[String]): Unit = {
    val jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)

    println(jedis.hget("xx","xx")==null)
  }


}
