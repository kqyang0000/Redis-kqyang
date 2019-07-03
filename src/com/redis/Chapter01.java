package com.redis;

import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;

    public static void main(String[] args) {

    }

    public void run() {
        Jedis conn = new Jedis("127.0.0.1");
        conn.select(15);
        // 文章发表
        String articleId = postArticle(conn, "username", "A title", "https://www.baidu.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println(" " + entry.getKey() + ":" + entry.getValue());
        }
        System.out.println();
        // 文章投票
        articleVote(conn, "other_user", articleId);
        // TODO
    }

    /**
     * 发表文章
     *
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        // 生成articleId
        String articleId = String.valueOf(conn.incr("article:"));
        // 以投票id为键user为值，添加到set集合，并设置投票过期时间（一周）
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);
        // 生成文章key
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        // 生成文章属性
        Map<String, String> articleData = new HashMap<>(16);
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        // 以文章为键文章属性为值，添加到hash散列表中
        conn.hmset(article, articleData);
        // 将文章投票分数和投票时间添加到zset中
        conn.zadd("score:" + articleId, now + VOTE_SCORE, article);
        conn.zadd("time:" + articleId, now, article);
        return articleId;
    }

    /**
     * 文章投票
     *
     * @param conn
     * @param user
     * @param articleId
     */
    public void articleVote(Jedis conn, String user, String articleId) {
        // 验证文章投票时间是否过期
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:" + articleId, "article:" + articleId) < cutoff) {
            System.out.println("此次投票已超出该文章 [article:" + articleId + "]投票截止日期，投票失败!");
            return;
        }
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, "article:" + articleId);
            conn.hincrBy("article:" + articleId, "votes", 1);
            System.out.println("投票成功!");
        }
    }
}
