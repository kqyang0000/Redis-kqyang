package com.redis.chapter;

import com.redis.common.Base;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ZParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Chapter03 extends Base {

    public static void main(String[] args) {
//        new Chapter03().new STRING();
//        new Chapter03().new LIST();
//        new Chapter03().new SET();
//        new Chapter03().new HASH();
//        new Chapter03().new ZSET();

        /*
         * 测试redis消息发布订阅
         */
        new Chapter03().run();
    }

    /**
     * 程序测试执行体
     */
    public void run() {
        /*
         * 订阅者
         */
        SubThread subThread = new SubThread();
        subThread.start();

        /*
         * 发布者
         */
        Publisher publisher = new Publisher();
        publisher.start();
    }

    /**
     * 字符串常用命令
     */
    public class STRING {
        private final String key = "str-key";

        public STRING() {
            run();
        }

        public void run() {
            // 1.自增（默认1）
            getConn().incr(key);
            // 2.自定义自增
            getConn().incrBy(key, 100);
            // 3.自减（默认1）
            getConn().decr(key);
            // 4.自定义自减
            getConn().decrBy(key, 100);
            // 5.浮点自增（v2.6 之后可用）
            getConn().incrByFloat(key, 3.1415926);
            // 6.值追加（会返回追加后的value长度）
            getConn().append(key, "6789");
            // 7.值截取（包含起始索引值）
            getConn().getrange(key, 1, 3);
            // 8.值替换（从第offset位置开始替换为指定的value，返回替换后的value长度）
            getConn().setrange(key, 5, "1234567");
            // 9.获取偏移量为offset的二进位的值
            getConn().getbit(key, 5);
            // 10.将位串中偏移量为offset的二进位值设置为value
            getConn().setbit(key, 0, "1");
            // 11.统计二进位中值为1的二进位数量，可选定范围（此范围为字符串索引范围）
            getConn().bitcount(key, 0, 0);
            // 12.按位运算操作（AND OR XOR NOT），返回运算后的字符长度
            getConn().bitop(BitOP.AND, "s-key", "s-key1", "s-key2");
        }
    }

    /**
     * 列表常用操作
     */
    public class LIST {
        private final String key = "list-key";

        public LIST() {
            run();
        }

        public void run() {
            // 1.将一个或多个值推入列表的左端，返回列表长度
            getConn().lpush(key, new String[]{"a", "b", "c", "d"});
            // 2.将一个或多个值推入列表的右端，返回列表长度
            getConn().rpush(key, new String[]{"e", "f", "g", "h"});
            // 3.移除并返回列表最左端的元素
            getConn().lpop(key);
            // 4.移除并返回列表最右端的元素
            getConn().rpop(key);
            // 5.返回列表中偏移量为offset的元素
            getConn().lindex(key, -1);
            // 6.返回范围内的列表元素，包含起始终止范围的元素
            getConn().lrange(key, 0, -1);
            // 7.队列表进行修剪，保留起至终止范围内的元素
            getConn().ltrim(key, 0, 4);
            // 8.从第一个非空列表中弹出位于最左侧的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            getConn().blpop(20, key);
            // 9.从第一个非空列表中弹出位于最右侧的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            getConn().brpop(20, key);
            // 10.从列表一中弹出位于最右端的元素，然后将这个元素推入列表二的最左端，并返回这个元素
            getConn().rpoplpush("l1", "l2");
            // 11.从列表一中弹出位于最右端的元素，然后将这个元素推入列表二的最左端，并返回这个元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            getConn().brpoplpush("l1", "l2", 20);
        }
    }

    /**
     * 集合操作
     */
    public class SET {
        private final String key = "set-key";

        public SET() {
            run();
        }

        public void run() {
            // 1.将一个或多个元素添加到集合里面，返回添加元素中并不存在于集合里面的数量
            getConn().sadd(key, "a", "b", "b");
            // 2.从集合中移除一个或多个元素，并返回移除元素的数量
            getConn().srem(key, "a", "b");
            // 3.检查元素是否存在于集合中
            getConn().sismember(key, "a");
            // 4.返回集合中包含元素的数量
            getConn().scard(key);
            // 5.返回集合包含的所有元素
            getConn().smembers(key);
            // 6.从集合里随机返回一个或多个元素，当count为正数时，返回的元素不会重复，为负数时可能会出现重复
            getConn().srandmember(key, 3);
            // 7.随机的移除一个或多个元素，并返回该元素
            getConn().spop(key, 2);
            // 8.从集合一中移除一个元素到集合二中，如果移除成功则返回1否则返回0
            getConn().smove("s1", "s2", "a");
            // 9.返回存在于第一集合但并不存在于其他集合中的元素（数学上的差集运算）
            getConn().sdiff("s1", "s2", "s3");
            // 10.将存在于第一集合但不存在于其他集合的元素存储在dest集合中
            getConn().sdiffstore("dest", "s1", "s2");
            // 11.返回同时存在于所有集合中的元素（数学上的交集运算）
            getConn().sinter("s1", "s2", "s3");
            // 12.返回同时存在于所有集合中的元素（数学上的交集运算）,并存储在dest集合中
            getConn().sinterstore("dest", "s1", "s2", "s3");
            // 13.返回至少存在于一个集合中的元素（数学上的并集）
            getConn().sunion("s1", "s2", "s3");
            // 14.返回至少存在于一个集合中的元素（数学上的并集）,并存储在dest集合中
            getConn().sunionstore("dest", "s1", "s2");
        }
    }

    /**
     * 散列
     */
    public class HASH {
        private final String key = "hash-key";

        public HASH() {
            run();
        }

        public void run() {
            // 1.为散列里面的一个或多个键设置值
            getConn().hmset(key, new HashMap<>(16));
            // 2.从散列里面获取一个或多个键的值
            getConn().hmget(key, "name:", "age:");
            // 3.删除散列里面的一个或多个键值对，返回成功找到并删除的键值对数量
            getConn().hdel(key, "name", "age");
            // 4.返回散列包含的键值对数量
            getConn().hlen(key);
            // 5.检查给定的键是否存在于hash散列中
            getConn().hexists(key, "name");
            // 6.获取散列包含的所有键
            getConn().hkeys(key);
            // 7.获取散列包含的所有值
            getConn().hvals(key);
            // 8.获取散列包含的所有键值对
            getConn().hgetAll(key);
            // 9.将键对应的值加上整数，会返回操作后的值
            getConn().hincrBy(key, "age", 1);
            // 10.将键对应的值加上浮点数，会返回操作后的值
            getConn().hincrByFloat(key, "weight", 2.1);
        }
    }

    /**
     * 有序集合
     */
    public class ZSET {
        private final String key = "zset-key";

        public ZSET() {
            run();
        }

        public void run() {
            // 1.将带有分值的成员添加到有序集合里面
            getConn().zadd(key, 100, "yimao");
            // 2.移除指定的成员，并返回移除的成员数量
            getConn().zrem(key, "yimao", "ermao");
            // 3.返回有序集合中成员数量
            getConn().zcard(key);
            // 4.给集合中成员加分
            getConn().zincrby(key, 100, "yimao");
            // 5.返回分值在min与max之间的成员数量
            getConn().zcount(key, 102, 105);
            // 6.返回成员在有序集合中的排名，成员按照分值从小到大排列 最小排名：0
            getConn().zrank(key, "yimao");
            // 7.返回成员分值
            getConn().zscore(key, "ermao");
            // 8.返回集合中指定范围内的成员
            getConn().zrange(key, 0, -1);
            // 9.返回有序集合成员排名，成员按照分值从大到小排列
            getConn().zrevrank(key, "yimao");
            // 10.返回有序集合给定排名范围内的成员，成员按分值从大到小排列
            getConn().zrevrange(key, 0, -1);
            // 11.返回有序集合中分值在min与max之间的所有成员
            getConn().zrangeByScore(key, 100, 105);
            // 12.返回有序集合中分值在min与max之间的所有成员，按照分值从大到小的顺序排列
            getConn().zrevrangeByScore(key, 105, 100);
            // 13.移除有序集合中排名在start与end之间的所有成员
            getConn().zremrangeByRank(key, 0, 1);
            // 14.移除有序集合中分值在min与max之间的所有成员
            getConn().zremrangeByScore(key, 100, 105);
            // 15.对给定的有序集合执行类似数学集合中的交集运算
            getConn().zinterstore("dest-key", new ZParams().aggregate(ZParams.Aggregate.MAX), "z1", "z2");
            // 16.对给定的有序集合执行类似数学集合中的并集运算
            getConn().zunionstore("dest-key", new ZParams().aggregate(ZParams.Aggregate.MAX), "z1", "z2");
        }
    }

    /**
     * 消息发布者
     */
    public class Publisher extends Thread {

        public Publisher() {
            super("PubThread");
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String line;
                try {
                    line = reader.readLine();
                    if (!"quit".equals(line)) {
                        getConn().publish("mychannel", line);
                    } else {
                        break;
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
            }
        }
    }

    /**
     * 消息订阅者
     */
    public class Subscriber extends JedisPubSub {
        public Subscriber() {
        }

        /**
         * 接收消息时会调用
         *
         * @param channel
         * @param message
         */
        @Override
        public void onMessage(String channel, String message) {
            printer("receive redis published message, channel is [" + channel + "], message is [" + message + "]");
        }

        /**
         * 订阅频道时会调用
         *
         * @param channel
         * @param subscribedChannels
         */
        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            printer("subscribe redis channel success, channel is [" + channel + "], subscribedChannels is [" + subscribedChannels + "]");
        }

        /**
         * 取消订阅频道时会调用
         *
         * @param channel
         * @param subscribedChannels
         */
        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            printer("unsubscribe redis channel success, channel is [" + channel + "], subscribedChannels is [" + subscribedChannels + "]");
        }
    }

    /**
     * 消息订阅线程
     */
    public class SubThread extends Thread {
        private final Subscriber subscriber = new Subscriber();
        private final String channel = "mychannel";

        public SubThread() {
            super("SubThread");
        }

        @Override
        public void run() {
            printer(String.format("subscribe redis, channel %s, thread will be blocked", channel));
            try {
                getConn().subscribe(subscriber, channel);
            } catch (Exception e) {
                printer(String.format("subscribe channel error, %s", e));
            }
        }
    }
}