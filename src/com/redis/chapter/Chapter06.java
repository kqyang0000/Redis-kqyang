package com.redis.chapter;

import com.redis.common.RedisHandler;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.*;

public class Chapter06 extends RedisHandler {
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    private static final Jedis conn = getConn();

    public static void main(String[] args) {
        new Chapter06().run();
    }

    public void run() {
        testAddUpdateContact();
    }

    /**
     * 自动补全最近联系人
     */
    public void testAddUpdateContact() {
        printer("\n----- testAddUpdateContact -----");
        conn.del("recent:user");

        printer("Let's add a few contacts");
        for (int i = 0; i < 10; i++) {
            addUpdateContact("user", "contact-" + ((int) Math.floor(i / 3)) + '-' + i);
        }
        printer("Current recently contacted contacts");
        List<String> contacts = conn.lrange("recent:user", 0, -1);
        for (String contact : contacts) {
            printer(" " + contact);
        }
        assert contacts.size() >= 10;
        printer();

        printer("Let's pull one of the older ones up to the front");
        addUpdateContact("user", "contact-1-4");
        contacts = conn.lrange("recent:user", 0, 2);
        printer("New top-3 contacts:");
        for (String contact : contacts) {
            printer(" " + contact);
        }
        assert "contact-1-4".equals(contacts.get(0));
        printer();

        printer("Let's remove a contact...");
        removeContactf("user", "contact-2-6");
        contacts = conn.lrange("recent:user", 0, -1);
        printer("New contacts:");
        for (String contact : contacts) {
            printer(" " + contact);
        }
        assert contacts.size() > 9;
        printer();

        printer("And Let's finally autocomplete on ");
        List<String> all = conn.lrange("recent:user", 0, -1);
        contacts = fetchAutocompleteList("user", "c");
        assert all.equals(contacts);
        List<String> equiv = new ArrayList<>(32);
        for (String contact : all) {
            if (contact.startsWith("contact-2-")) {
                equiv.add(contact);
            }
        }
        contacts = fetchAutocompleteList("user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contacts);
        assert equiv.equals(contacts);
        conn.del("recent:user");
    }

    /**
     * 通讯录自动补全功能
     */
    public void testAddressBookAutocomplete() {
        printer("\n----- testAddressBookAutocomplete -----");
        conn.del("members:test");
        printer("the start/end range of 'abc' is: " + Arrays.toString(findPrefixRange("abc")));
        printer();

        printer("Let's add a few people to the guild");
        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}) {
            joinGuild("test", name);
        }
        printer();
        printer("now let's try to find users with names starting with 'je':");
    }

    /**
     * 获取前驱/后继
     *
     * @param prefix
     * @return
     */
    public String[] findPrefixRange(String prefix) {
        int pos = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        char suffix = VALID_CHARACTERS.charAt(pos > 0 ? pos - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        return new String[]{start, end};
    }

    /**
     * 添加联系人到通讯录
     *
     * @param guild
     * @param user
     */
    public void joinGuild(String guild, String user) {
        conn.zadd("members:" + guild, 0, user);
    }

    /**
     * 通讯录自动补全
     *
     * @param guild
     * @param prefix
     * @return
     */
    public Set<String> autocompleteOnPrefix(String guild, String prefix) {
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        String identifier = getUUID();
        start += identifier;
        end += identifier;
        String zsetName = "members:" + guild;

        /*
         * 将两个带有前驱和后继的元素插入到有序集合中，这样一来所在范围内的所有元素就会被包含在先后插入的两个元素之间，
         * 在findPrefixRange方法中在前驱和后继拼接'{'是为了过滤掉有多个相同前缀查询时被插入有序集合中的前驱和后继，
         * identifier在此处也是为了区别开多个相同前缀同时查询带来的问题
         */
        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true) {
            conn.watch(zsetName);

        }

    }

    /**
     * 添加或更新最近联系人
     *
     * @param user
     * @param contact
     */
    public void addUpdateContact(String user, String contact) {
        String acList = "recent:" + user;
        /*
         * 为解决资源竞争，故此处带事务执行
         *
         * 从列表移除一个元素，如果:
         *        1.count > 0 ，从列表表头开始查找，移除count个相同元素
         *        2.count = 0 ，移除所有相同元素
         *        3.count < 0 ，从列表表尾开始查找，移除绝对值count个相同元素
         *
         * 在列表最左侧添加，然后保留100个元素
         */
        Transaction trans = conn.multi();
        trans.lrem(acList, 0, contact);
        trans.lpush(acList, contact);
        trans.ltrim(acList, 0, 99);
        trans.exec();
    }

    /**
     * 移除一个最近联系人
     *
     * @param user
     * @param contact
     */
    public void removeContactf(String user, String contact) {
        conn.lrem("recent:" + user, 0, contact);
    }

    /**
     * 获取匹配前缀的列表
     *
     * @param user
     * @param prefix
     * @return
     */
    public List<String> fetchAutocompleteList(String user, String prefix) {
        List<String> candidates = conn.lrange("recent:" + user, 0, -1);
        List<String> matches = new ArrayList<>(32);
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix)) {
                matches.add(candidate);
            }
        }
        return matches;
    }
}