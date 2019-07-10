package com.redis.chapter;

import com.redis.common.Base;
import redis.clients.jedis.BitOP;

public class Chapter03 extends Base {

    public static void main(String[] args) {
        new Chapter03().new STRING();
        new Chapter03().new LIST();
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
            conn.incr(key);
            // 2.自定义自增
            conn.incrBy(key, 100);
            // 3.自减（默认1）
            conn.decr(key);
            // 4.自定义自减
            conn.decrBy(key, 100);
            // 5.浮点自增（v2.6 之后可用）
            conn.incrByFloat(key, 3.1415926);
            // 6.值追加（会返回追加后的value长度）
            conn.append(key, "6789");
            // 7.值截取（包含起始索引值）
            conn.getrange(key, 1, 3);
            // 8.值替换（从第offset位置开始替换为指定的value，返回替换后的value长度）
            conn.setrange(key, 5, "1234567");
            // 9.获取偏移量为offset的二进位的值
            conn.getbit(key, 5);
            // 10.将位串中偏移量为offset的二进位值设置为value
            conn.setbit(key, 0, "1");
            // 11.统计二进位中值为1的二进位数量，可选定范围（此范围为字符串索引范围）
            conn.bitcount(key, 0, 0);
            // 12.按位运算操作（AND OR XOR NOT），返回运算后的字符长度
            conn.bitop(BitOP.AND, "s-key", "s-key1", "s-key2");
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
            conn.lpush(key, new String[]{"a", "b", "c", "d"});
            // 2.将一个或多个值推入列表的右端，返回列表长度
            conn.rpush(key, new String[]{"e", "f", "g", "h"});
            // 3.移除并返回列表最左端的元素
            conn.lpop(key);
            // 4.移除并返回列表最右端的元素
            conn.rpop(key);
            // 5.返回列表中偏移量为offset的元素
            conn.lindex(key, -1);
            // 6.返回范围内的列表元素，包含起始终止范围的元素
            conn.lrange(key, 0, -1);
            // 7.队列表进行修剪，保留起至终止范围内的元素
            conn.ltrim(key, 0, 4);
            // 8.从第一个非空列表中弹出位于最左侧的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            conn.blpop(20, key);
            // 9.从第一个非空列表中弹出位于最右侧的元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            conn.brpop(20, key);
            // 10.从列表一中弹出位于最右端的元素，然后将这个元素推入列表二的最左端，并返回这个元素
            conn.rpoplpush("l1", "l2");
            // 11.从列表一中弹出位于最右端的元素，然后将这个元素推入列表二的最左端，并返回这个元素，或者在timeout秒之内阻塞并等待可弹出的元素出现（有值则立刻弹出，否则阻塞等待）
            conn.brpoplpush("l1", "l2", 20);
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
            // 1.
        }
    }
}