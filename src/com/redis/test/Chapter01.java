package com.redis.test;

import com.redis.common.Base;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 extends Base {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        /*
         * 文章发表
         */
        String articleId = postArticle("username", "A title", "https://www.baidu.com");
        printer("We posted a new article with id: " + articleId);
        printer("Its HASH looks like:");
        /*
         * 获取文章详情
         */
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            printer(entry.getKey() + ":" + entry.getValue());
        }
        printer();
        /*
         * a.投票
         * b.打印投票数
         */
        articleVote("other_user", articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        printer("We voted for the article, it now has votes:" + votes);
        assert Integer.parseInt(votes) > 1;
        /*
         * 打印文章列表
         */
        printer("The current highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(1, "score:" + articleId);
        printArticles(articles);
        assert articles.size() >= 1;
        /*
         * 添加分组
         */
        addGroups(articleId, new String[]{"new-group"});
        /*
         * 获取组内文章
         */
        printer("We added the article to a new group, other articles include:");
        articles = getGroupArticles("new-group", 1, articleId);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发表文章
     *
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(String user, String title, String link) {
        String articleId = String.valueOf(conn.incr("article:"));
        /*
         * 以投票id为键user为值，添加到set集合，并设置投票过期时间（一周）
         */
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);
        long now = System.currentTimeMillis() / 1000;
        String articleKey = "article:" + articleId;
        /*
         * a.生成文章属性
         * b.以文章为键文章属性为值，添加到hash散列表中
         */
        Map<String, String> articleData = new HashMap<>(16);
        articleData.put("id", articleId);
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(articleKey, articleData);
        /*
         * 将文章投票分数和投票时间添加到zset中
         */
        conn.zadd("score:" + articleId, now + VOTE_SCORE, articleKey);
        conn.zadd("time:" + articleId, now, articleKey);
        return articleId;
    }

    /**
     * 文章投票
     *
     * @param user
     * @param articleId
     */
    public void articleVote(String user, String articleId) {
        // 验证文章投票时间是否过期
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        if (conn.zscore("time:" + articleId, "article:" + articleId) < cutoff) {
            printer("此次投票已超出该文章 [article:" + articleId + "]投票截止日期，投票失败!");
            return;
        }
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:" + articleId, VOTE_SCORE, "article:" + articleId);
            conn.hincrBy("article:" + articleId, "votes", 1);
            printer("投票成功!");
        }
    }

    /**
     * 获取所有文章
     *
     * @param page
     * @return
     */
    public List<Map<String, String>> getArticles(int page, String key) {
        return getArticles(page, key, null);
    }

    /**
     * 获取所有文章
     *
     * @param page
     * @param key
     * @param articleId
     * @return
     */
    public List<Map<String, String>> getArticles(int page, String key, String articleId) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;
        // 按照分值排序
        Set<String> articleIds = conn.zrevrange(key, start, end);
        List<Map<String, String>> articles = new LinkedList<>();
        for (String id : articleIds) {
            Map<String, String> articleData = conn.hgetAll(id);
            articles.add(articleData);
        }
        return articles;
    }

    /**
     * 打印文章列表
     *
     * @param articles
     */
    public void printArticles(List<Map<String, String>> articles) {
        for (Map<String, String> article : articles) {
            printer("id:" + article.get("id"));
            for (Map.Entry<String, String> entry : article.entrySet()) {
                if (entry.getKey().equals("id")) {
                    continue;
                }
                printer(entry.getKey() + ":" + entry.getValue());
            }
        }
    }

    /**
     * 添加分组
     *
     * @param articleId
     * @param groups
     */
    public void addGroups(String articleId, String[] groups) {
        String article = "article:" + articleId;
        for (String group : groups) {
            conn.sadd("group:" + group, article);
        }
    }

    /**
     * 获取组内文章
     *
     * @param group
     * @param page
     * @param articleId
     * @return
     */
    public List<Map<String, String>> getGroupArticles(String group, int page, String articleId) {
        return getGroupArticles(group, page, "score:", articleId);
    }

    /**
     * 获取组内文章
     *
     * @param group
     * @param page
     * @param score
     * @param articleId
     * @return
     */
    public List<Map<String, String>> getGroupArticles(String group, int page, String score, String articleId) {
        String key = score + group;
        if (!conn.exists(key)) {
            ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, zParams, "group:" + group, score + articleId);
            conn.expire(key, 60);
        }
        return getArticles(page, key);
    }
}