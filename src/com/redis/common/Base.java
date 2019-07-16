package com.redis.common;

import java.util.UUID;

/**
 * @author kqyang
 * @date 2019-7-16 16:41:58
 */
public class Base extends Logger{

    /**
     * 生成随机数
     *
     * @return
     */
    protected String getUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * 会话打印
     *
     * @param messages
     */
    protected void printer(String... messages) {
        if (messages != null && messages.length > 0) {
            for (String mm : messages) {
                System.out.print(mm);
            }
            System.out.println();
        } else {
            System.out.println();
        }
    }
}
