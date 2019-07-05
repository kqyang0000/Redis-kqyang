package com.redis;

import redis.clients.jedis.Jedis;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class Chapter02 {

    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run() throws InterruptedException {
        Jedis conn = new Jedis("127.0.0.1", 6379);
        conn.select(14);
        testLoginCookies(conn);
    }

    /**
     * 测试登录cookies
     *
     * @param conn
     */
    public void testLoginCookies(Jedis conn) throws InterruptedException {
        System.out.println("----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();
        /*
         * 更新token
         */
        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token:" + token);
        System.out.println("For user:'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String user = checkToken(conn, token);
        System.out.println(user);
        System.out.println();
        assert user != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");
        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean session thread is still alive!");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is:" + s);
        assert s == 0;
    }

    /**
     * 更新token
     *
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);
        conn.zadd("recent:", timestamp, token);
        if (item != null) {
            conn.zadd("viewed:" + token, timestamp, item);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
        }
    }

    /**
     * 检查token
     *
     * @param conn
     * @param token
     * @return
     */
    public String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    /**
     * 清理session线程
     */
    public class CleanSessionsThread extends Thread {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
            this.conn = new Jedis("127.0.0.1", 6379);
            this.conn.select(14);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                List<Object> sessionKeys = new LinkedList<>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }
}
