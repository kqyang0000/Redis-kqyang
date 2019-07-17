package com.redis.chapter;

import com.redis.common.RedisHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

public class Chapter05 extends RedisHandler {
    private static final Jedis conn = getConn();
    private static final Collator COLLATOR = Collator.getInstance();
    public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    public static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");

    static {
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static void main(String[] args) {
        new Chapter05().run();
    }

    public void run() {
//        clearKeys();
//        testLogRecent();
        testLogCommon();
    }

    public void testLogRecent() {
        printer("\n----- testLogRecent -----");
        printer("Let's write a few logs to the recent log");
        for (int i = 0; i < 5; i++) {
            logRecent("test", "this is message " + (i + 1));
        }
    }

    /**
     * 日志打印
     *
     * @param name
     * @param message
     */
    public void logRecent(String name, String message) {
        logRecent(name, message, INFO);
    }

    /**
     * 日志打印
     *
     * @param name
     * @param message
     * @param severity
     */
    public void logRecent(String name, String message, String severity) {
        String destination = "recent:" + name + ":" + severity;
        Pipeline pipe = conn.pipelined();
        pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message);
        // 仅保留一百条日志
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }

    public void testLogCommon() {
        printer("\n----- testLogCommon -----");
        printer("Let's write some items to the common log");
        for (int count = 0; count < 6; count++) {
            for (int i = 0; i < count; i++) {
                logCommon("test", "message-" + count);
            }
        }
        Set<Tuple> common = conn.zrevrangeWithScores("common:test:info", 0, -1);
        printer("The current number of common messages is: " + common.size());
        printer("Those common messages are:");
        for (Tuple tuple : common) {
            printer(" " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert common.size() >= 5;
    }

    public void logCommon(String name, String message) {
        logCommon(name, message, INFO, 5000);
    }

    public void logCommon(String name, String message, String severity, int timeout) {
        String commonDest = "common:" + name + ":" + severity;
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end) {
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());
            String existing = conn.get(startKey);

            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0) {
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + ":pstart");
                trans.set(startKey, hourStart);
            }

            trans.zincrby(commonDest, 1, message);

            String recentDest = "recent:" + name + ":" + severity;
            trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to the watched key changing.
            if (results == null) {
                continue;
            }
            return;
        }
    }
}










