package com.redis.chapter;

import com.redis.common.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Chapter04 extends Base {

    public static void main(String[] args) {
        new Chapter04().run();
    }

    public void run() {
        /*
         * 上架
         */
        testListItem(false);
        /*
         * 购买
         */
        testPurchaseItem();
        /*
         *
         */
        testBenchmarkUploadToken();
    }

    public void testListItem(boolean nested) {
        Jedis conn = getConn();
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
        Jedis conn = getConn();
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
        Jedis conn = getConn();
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
        Jedis conn = getConn();
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
        benchmarkUploadToken(5);
    }

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
        }
    }
}
















