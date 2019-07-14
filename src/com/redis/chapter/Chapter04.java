package com.redis.chapter;

import com.redis.common.Base;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public class Chapter04 extends Base {

    public static void main(String[] args) {
        new Chapter04().run();
    }

    public void run() {
        testListItem(false);
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
}
















