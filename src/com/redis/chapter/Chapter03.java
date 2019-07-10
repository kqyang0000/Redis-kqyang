package com.redis.chapter;

import com.redis.common.Base;
import redis.clients.jedis.BitOP;

public class Chapter03 extends Base {

    public static void main(String[] args) {
        new Chapter03().new STRING();
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
}