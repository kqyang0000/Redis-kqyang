package com.redis.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

/**
 * Created by kqyang on 2019/7/7.
 */
public class RedisHandler extends Base {
    private static final String PASSWORD = "";
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int INDEX = 14;
    private static final int TIMEOUT = 2000;
    private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), LOCAL_HOST, PORT, TIMEOUT);

    /**
     * 获取redis客户端链接
     *
     * @return
     */
    protected static Jedis getConn() {
        Jedis conn = jedisPool.getResource();
        conn.select(INDEX);
        return conn;
    }

    /**
     * 关闭客户端链接
     *
     * @param conn
     */
    protected void returnConn(Jedis conn) {
        if (conn != null && jedisPool != null) {
            jedisPool.returnResource(conn);
        }
    }

    /**
     * 生成token
     */
    protected String getToken() {
        return super.getUUID();
    }

    /**
     * 清空库
     */
    protected void clearKeys() {
        Set<String> keys = getConn().keys("*");
        if (keys != null && keys.size() > 0) {
            getConn().del(keys.toArray(new String[keys.size()]));
            printer("already clear");
        }
    }
}