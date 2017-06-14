package redis

import redis.clients.jedis.Jedis
import utils.{RedisClient, RedisProperties}

/**
  * Created by slview on 17-6-7.
*/
object RedisTest {
  def main(args: Array[String]): Unit = {
    val jedis = RedisClient.pool.getResource
    println(RedisProperties.REDIS_PORT)
    println(RedisProperties.REDIS_SERVER)
    println(RedisProperties.REDIS_PASSWORD)

  // val jedis = new Jedis("cdh-dn1",6379)

    //jedis.auth("")
    //jedis.set("data::dir","/home/data/hdfs/aaa.txt")
    //println(jedis.get("data::dir"))
    //jedis.hincrBy("test::QOE","qoetest",888888888L)
    //jedis.hset("qoe::cdn::location","cdn","/hadoop/qoe/cdninfo/cdnnodeinfo.txt")
    //jedis.hset("qoe::cdn::location","server","/hadoop/qoe/cdninfo/cdnserverinfo.txt")

    println(jedis.hget("qoe::cdn::location","cdn"))
    println(jedis.hget("qoe::cdn::location","server"))
  }
}
