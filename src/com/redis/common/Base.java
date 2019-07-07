package com.redis.common;

import redis.clients.jedis.Jedis;

import java.util.UUID;

/**
 * Created by kqyang on 2019/7/7.
 */
public class Base {
    private static final String PASSWORD = "";
    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int PORT = 6379;
    private static final int INDEX = 14;
    protected static Jedis conn = null;

    public Base() {
        Jedis conn = new Jedis(LOCAL_HOST, PORT);
//        conn.auth(PASSWORD);
        conn.select(INDEX);
        this.conn = conn;
    }

    /**
     * 生成token
     */
    public String getToken() {
        return UUID.randomUUID().toString();
    }

    /**
     * 会话打印
     *
     * @param messages
     */
    public void printer(String... messages) {
        if (messages != null && messages.length > 0) {
            for (String mm : messages) {
                System.out.print(mm);
            }
            System.out.println();
        } else {
            System.out.println();
        }
    }
}