package com.redis.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;
import java.util.UUID;

/**
 * Created by kqyang on 2019/7/7.
 */
public class Base {
    private static final String PASSWORD = "";
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int INDEX = 14;
    private static final int TIMEOUT = 2000;
    private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), LOCAL_HOST, PORT, TIMEOUT);
    private static Jedis conn = null;

    /**
     * 获取redis客户端链接
     *
     * @return
     */
    protected Jedis getConn() {
        conn = jedisPool.getResource();
        conn.select(INDEX);
        return conn;
    }

    /**
     * 生成token
     */
    protected String getToken() {
        return UUID.randomUUID().toString();
    }

    /**
     * 会话打印
     *
     * @param messages
     */
    protected void printer(String... messages) {
        if (messages != null && messages.length > 0) {
            for (String mm : messages) {
                System.out.print(mm);
            }
            System.out.println();
        } else {
            System.out.println();
        }
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