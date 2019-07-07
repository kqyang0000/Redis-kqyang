package com.redis.test;

import com.redis.common.Base;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Chapter02 extends Base {

    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run() throws InterruptedException {
        /*
         * 登陆测试
         */
        testLoginCookies();
        /*
         * 购物车测试
         */
        testShoppingCartCookies();
    }

    /**
     * 测试登录cookies
     */
    public void testLoginCookies() throws InterruptedException {
        printer("----- testLoginCookies -----");
        String token = getToken();
        /*
         * 更新token
         */
        updateToken(token, "username", "itemX");
        printer("We just logged-in/updated token:" + token);
        printer("For user:'username'");
        printer();

        /*
         * 验证token
         */
        printer("What username do we get when we look-up that token?");
        String user = checkToken(token);
        printer(user);
        printer();
        assert user != null;

        /*
         * 缓存清理
         */
        printer("Let's drop the maximum number of cookies to 0 to clean them out");
        printer("We will start a thread to do the cleaning, while we stop it later");
        CleanSessionsThread thread = new CleanSessionsThread(10000000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean session thread is still alive!");
        }

        long s = conn.hlen("login:");
        printer("The current number of sessions still available is:" + s);
        assert s == 0;
    }

    /**
     * 测试购物车
     */
    public void testShoppingCartCookies() {
        printer("\n----- testShoppingCartCookies -----");
        String token = getToken();
    }

    /**
     * 更新token
     *
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(String token, String user, String item) {
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
     * @param token
     * @return
     */
    public String checkToken(String token) {
        return conn.hget("login:", token);
    }

    /**
     * 清理session线程
     */
    public class CleanSessionsThread extends Thread {
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
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