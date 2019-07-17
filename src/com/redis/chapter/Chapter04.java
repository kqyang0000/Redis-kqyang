package com.redis.chapter;

import com.redis.common.RedisHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Chapter04 extends RedisHandler {
    private static final Jedis conn = getConn();

    public static void main(String[] args) {
        new Chapter04().run();
    }

    public void run() {
        /*
         * 上架
         */
//        testListItem(false);
        /*
         * 购买
         */
//        testPurchaseItem();
        /*
         * 测试流水线方式执行redis命令
         */
        testBenchmarkUploadToken();
    }

    public void testListItem(boolean nested) {
        if (!nested) {
            printer("\n----- testListItem -----");
        }

        printer("We need to set up just enough state so that a user can list an item");
        String seller = "user0";
        String item = "item0";
        conn.sadd("inventory:" + seller, item);

        Set<String> i = conn.smembers("inventory:" + seller);
        printer("The user's inventory has:");
        for (String member : i) {
            printer(" " + member);
        }
        assert i.size() > 0;
        printer();

        printer("Listing the item...");
        boolean l = listItem(item, seller, 10);
        printer("Listing the item succeeded? " + l);
        assert l;

        Set<Tuple> r = conn.zrangeWithScores("market:", 0, -1);
        printer("The market contains:");
        for (Tuple tuple : r) {
            printer(" " + tuple.getElement() + "," + tuple.getScore());
        }
        assert r.size() > 0;
    }

    /**
     * 物品上架
     *
     * @param itemId
     * @param sellerId
     * @param price
     * @return
     */
    public boolean listItem(String itemId, String sellerId, double price) {
        String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            conn.watch(inventory);
            if (!conn.sismember(inventory, itemId)) {
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);
            List<Object> results = trans.exec();
            /*
             * null response indicates that the transaction was aborted due to the watched key changing
             */
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }

    public void testPurchaseItem() {
        printer("\n----- testPurchaseItem -----");
        testListItem(true);

        printer("We need to set up just enough state so a user buy an item");
        conn.hset("users:user1", "funds", "125");
        Map<String, String> r = conn.hgetAll("users:user1");
        printer("The user has some money:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            printer(" " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;
        printer();

        printer("Let's purchase an item");
        boolean b = purchaseItem("user1", "item0", "user0", 10);
        printer("Purchase an item succeeded? " + b);
        assert b;
        r = conn.hgetAll("users:user1");
        printer("Their money is now:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            printer(" " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        String buyer = "user1";
        Set<String> i = conn.smembers("inventory:" + buyer);
        printer("Their inventory is now:");
        for (String member : i) {
            printer(" " + member);
        }
        assert i.size() > 0;
        assert i.contains("item0");
        assert conn.zscore("market:", "item0.user0") == null;


    }

    /**
     * 商品购买
     *
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     */
    public boolean purchaseItem(String buyerId, String itemId, String sellerId, double lprice) {
        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + "." + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end) {
            conn.watch("market:", buyer);
            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if (price != lprice || price > funds) {
                conn.unwatch();
                return false;
            }

            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", (int) price);
            trans.hincrBy(buyer, "funds", (int) -price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            /*
             * null response indicates that the transaction was aborted due to the watched key changing.
             */
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }

    public void testBenchmarkUploadToken() {
        printer("\n----- testBenchmarkUpdate -----");
        benchmarkUploadToken(30);
    }

    /**
     * 测试redis 流水线执行方式
     * 带宽: 10M   cup: 4核   内存: 16G
     * ---------------------------total---time-avg---
     * in 5s: updateToken         5992    5    1198  |
     *        updateTokenPipeline 132622  5    26524 |
     * ---------------------------total---time-avg---|
     * in 15s: updateToken        18373   15   1224  |
     *        updateTokenPipeline 470706  15   31380 |
     * ---------------------------total---time-avg---|
     * in 30s: updateToken        39521   30   1317  |
     *        updateTokenPipeline 922061  30   30735 |
     * ----------------------------------------------
     */
    public void benchmarkUploadToken(int duration) {
        try {
            Class[] args = new Class[]{
                    String.class,
                    String.class,
                    String.class
            };
            Method[] methods = new Method[]{
                    this.getClass().getDeclaredMethod("updateToken", args),
                    this.getClass().getDeclaredMethod("updateTokenPipeline", args)
            };
            for (Method method : methods) {
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end) {
                    count++;
                    method.invoke(this, "token" + count, "user" + count, "item" + count);
                }
                long delta = System.currentTimeMillis() - start;
                printer(method.getName() + ' ' + count + ' ' + (delta / 1000) + ' ' + (count / (delta / 1000)));
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 分步执行
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
            conn.zadd("viewed:" + token, timestamp, token);
            conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 流水线方式执行
     *
     * @param token
     * @param user
     * @param item
     *
     * 带事物     pipe.multi() --> pipe.exec()
     * 不带事物   pipe.sync()
     */
    public void updateTokenPipeline(String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = conn.pipelined();
        pipe.multi();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zadd("viewed:" + token, timestamp, token);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }
        pipe.exec();
    }
}