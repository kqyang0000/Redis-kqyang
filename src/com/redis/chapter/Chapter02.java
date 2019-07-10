package com.redis.chapter;

import com.google.gson.Gson;
import com.redis.common.Base;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class Chapter02 extends Base {

    public static void main(String[] args) throws InterruptedException {
        new Chapter02().run();
    }

    public void run() throws InterruptedException {
        /*
         * 登陆测试
         */
//        testLoginCookies();
        /*
         * 购物车测试
         */
//        testShoppingCartCookies();
        /*
         * 缓存网页测试
         */
//        testCacheRequest();
        /*
         * 数据行缓存测试
         */
        testCacheRows();
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
    public void testShoppingCartCookies() throws InterruptedException {
        printer("\n----- testShoppingCartCookies -----");
        String token = getToken();
        /*
         * 刷新token
         */
        printer("We'll refresh our session...");
        updateToken(token, "username", "itemY");

        /*
         * 添加一个商品到购物车
         */
        printer("And add an item to the shopping cart");
        addToCart(token, "itemY", 3);

        /*
         * 获取购物车中所有商品
         */
        Map<String, String> items = conn.hgetAll("cart:" + token);
        printer("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : items.entrySet()) {
            printer(entry.getKey() + ":" + entry.getValue());
        }
        printer();
        assert items.size() >= 1;

        /*
         * 清理购物车
         */
        printer("Let's clean out our sessions and carts");
        CleanFullSessionThread thread = new CleanFullSessionThread(10000000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive!");
        }

        /*
         * 获取购物车中所有商品
         */
        items = conn.hgetAll("cart:" + token);
        printer("Our shopping cart now contains");
        for (Map.Entry<String, String> entry : items.entrySet()) {
            printer(entry.getKey() + ":" + entry.getValue());
        }
        assert items.size() == 0;
    }

    /**
     * 测试缓存网页
     */
    public void testCacheRequest() {
        printer("\n----- testCacheRequest -----");
        String token = getToken();

        Callback callback = new Callback() {
            @Override
            public String call(String request) {
                return "content for " + request;
            }
        };

        updateToken(token, "username", "itemX");
        String url = "http://chapter.com/?item=itemX";
        printer("We are going to cache a simple request against " + url);
        String result = cacheRequest(url, callback, token);
        printer("We got initial content:\n" + result);
        printer();
        assert result != null;

        printer("To chapter that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(url, null, token);
        printer("We ended up getting the same response!\n" + result2);
        assert result.equals(result2);
        assert canCache("http://chapter.com/", token);
        assert canCache("http://chapter.com/?item=itemX&_=123456", token);
    }

    /**
     * 测试缓存行数据
     */
    public void testCacheRows() throws InterruptedException {
        printer("\n----- testCacheRows -----");
        printer("First, let's schedule caching of itemX every 5 seconds");

        /*
         * 缓存行
         */
        scheduleRowCache("itemX", 5);
        printer("Our schedule looks like:");

        /*
         * 打印行
         */
        Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            printer(tuple.getElement() + ", " + (float) tuple.getScore());
        }
        assert s.size() != 0;

        printer("We'll start a caching thread that will cache the data...");
        CacheRowsThread thread = new CacheRowsThread();
        thread.start();

        Thread.sleep(1000);
        printer("Our cached data looks like:");
        String r = conn.get("inv:itemX");
        printer(r);
        assert r != null;
        printer();

        printer("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        printer("Notice that the data has changed...");
        String r2 = conn.get("inv:itemX");
        printer(r2);
        printer();
        assert r2 != null;
        assert !r.equals(r2);

        printer("Let's force un-caching");
        scheduleRowCache("itemX", -1);
        Thread.sleep(1000);
        r = conn.get("inv:itemX");
        printer("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The database caching thread is still alive!");
        }

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
            /*
             * 移除排序集合中区间内的成员
             *     eg:  1  2  3  4  5  6  7  8  9
             *  index:  0  1  2  3  4  5  6  7  8
             * -index: -9 -8 -7 -6 -5 -4 -3 -2  -1
             * conn.zremrangeByRank(key,0,-7)
             * result:           4  5  6  7  8  9
             * formula: 0,-(saveCount+1)
             */
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            /*
             * 网页分析，新添加的代码
             * 作用：用户浏览量越多，则score值越小，则当前商品越在有序集合上面的位置
             */
            conn.zincrby("viewed:", -1, item);
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

    /**
     * 添加商品到购物车
     *
     * @param session
     * @param item
     * @param count
     */
    public void addToCart(String session, String item, int count) {
        if (count <= 0) {
            conn.hdel("cart:" + session, item);
        } else {
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
    }

    /**
     * 清理购物车线程
     */
    public class CleanFullSessionThread extends Thread {
        private int limit;
        private boolean quit;

        public CleanFullSessionThread(int limit) {
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

                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                List<String> sessionKeys = new LinkedList<>();
                for (String session : sessions) {
                    sessionKeys.add("viewed:" + session);
                    sessionKeys.add("cart:" + session);
                }

                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }

    /**
     * 回调接口
     */
    public interface Callback {
        String call(String request);
    }

    /**
     * 缓存请求
     *
     * @param request
     * @param callback
     * @param token
     * @return
     */
    public String cacheRequest(String request, Callback callback, String token) {
        if (!canCache(request, token)) {
            return callback != null ? callback.call(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);

        if (content == null && callback != null) {
            content = callback.call(request);
            conn.setex(pageKey, 300, content);
        }
        return content;
    }

    /**
     * 验证是否可以缓存
     *
     * @param request
     * @param token
     * @return
     */
    public boolean canCache(String request, String token) {
        try {
            URL url = new URL(request);
            Map<String, String> params = new HashMap<>(16);
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            Long rank = conn.zrank("viewed:" + token, itemId);
            return rank != null && rank < 10000;
        } catch (MalformedURLException e) {
            printer("canCache method exception: " + e.getMessage());
            return false;
        }
    }

    /**
     * 提取商品id
     *
     * @param params
     * @return
     */
    public String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    /**
     * 判断是否为动态参数
     *
     * @param params
     * @return
     */
    public boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    /**
     * 获取hash值
     *
     * @param request
     * @return
     */
    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    /**
     * 缓存行
     *
     * @param rowId
     * @param delay
     */
    public void scheduleRowCache(String rowId, int delay) {
        conn.zadd("delay:", delay, rowId);
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }

    /**
     * 缓存行线程
     */
    public class CacheRowsThread extends Thread {
        private boolean quit;

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            Gson gson = new Gson();
            while (!quit) {
                Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now) {
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                String rowId = next.getElement();
                Double delay = conn.zscore("delay:", rowId);
                if (delay <= 0) {
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    /**
     * 库存实体
     */
    public static class Inventory {
        private String id;
        private String data;
        private long time;

        private Inventory(String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}