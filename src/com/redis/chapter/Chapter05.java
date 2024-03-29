package com.redis.chapter;

import com.google.gson.Gson;
import com.redis.common.RedisHandler;
import javafx.util.Pair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

public class Chapter05 extends RedisHandler {
    private static final Jedis conn = getConn();
    private static final Collator COLLATOR = Collator.getInstance();
    public static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    public static final SimpleDateFormat ISO_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");
    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    static {
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static void main(String[] args) throws InterruptedException {
        new Chapter05().run();
    }

    public void run() throws InterruptedException {
        testLogRecent();
        testLogCommon();
        testCounters();
        testIpLookup();

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

    /**
     * 网页计数器
     */
    public void testCounters() throws InterruptedException {
        printer("\n----- testCounters -----");
        printer("Let's update some counters for now and a little in the future");
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < 10; i++) {
            int count = new Random().nextInt() * 5 + 1;
            updateCounter("test", count, now + i);
        }

        List<Pair<Integer, Integer>> counter = getCounter("test", 1);
        printer("We have some per-second counters: " + counter.size());
        printer("These counters include:");
        for (Pair<Integer, Integer> pair : counter) {
            printer(" " + pair);
        }
        assert counter.size() >= 10;

        counter = getCounter("test", 5);
        printer("We have some per-5-second counters: " + counter.size());
        printer("These counters include:");
        for (Pair<Integer, Integer> pair : counter) {
            printer(" " + pair);
        }
        assert counter.size() >= 2;
        printer();

        printer("Let's clean out some counters by setting our sample count to 0");
        CleanCountersThread thread = new CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter("test", 86400);
        printer("Did we clean out all of the counters? " + (counter.size() == 0));
        assert counter.size() == 0;
    }

    public void updateCounter(String name, int count) {
        updateCounter(name, count, System.currentTimeMillis() / 1000);
    }

    /**
     * 更新计数器
     *
     * @param name
     * @param count
     * @param now
     */
    public void updateCounter(String name, int count, long now) {
        Transaction trans = conn.multi();
        for (int prec : PRECISION) {
            /*
             * 用于计算当前时间片的起始时间
             *
             * 例如：prec=5; now=126
             *      pnow=int(126/5)*5=121
             *
             * 例如：prec=10; now=126
             *      pnow=int(126/10)*10=120
             */
            long pnow = (now / prec) * prec;
            /*
             * 拼接口的key例如：1:test
             *               5:test
             *               60:test
             *               300:test
             *               3600:test
             *               18000:test
             *               86400:test
             */
            String prec_member = String.valueOf(prec) + ':' + name;
            /*
             * 将所有时间片添加到有序集合中，默认分值设置为0
             * TODO 暂时没看出来有什么用
             */
            trans.zadd("known:", 0, prec_member);
            /*
             * 记录当前时间片内的点击数量，属性为当前时间片的起始时间，对应的属性值在一定时间片内会随着
             * 点击量的增加而更新
             */
            trans.hincrBy("count:" + prec_member, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    /**
     * 获取时间片内页面点击次数
     *
     * @param name
     * @param precision
     * @return
     */
    public List<Pair<Integer, Integer>> getCounter(String name, int precision) {
        String prec_member = String.valueOf(precision) + ":" + name;
        /*
         * 获取时间片内每时段页面点击次数
         *
         * 例如：count:30:test 20100021500 78
         * 例如：count:30:test 20100021530 122
         * 例如：count:30:test 20100021560 15
         * 例如：count:30:test 20100021590 33
         */
        Map<String, String> date = conn.hgetAll("count:" + prec_member);
        List<Pair<Integer, Integer>> results = new ArrayList<>(32);
        for (Map.Entry<String, String> entry : date.entrySet()) {
            Pair pair = new Pair<>(Integer.parseInt(entry.getKey()), Integer.parseInt(entry.getValue()));
            results.add(pair);
        }
        /*
         * 对集合按时间戳进行排序
         *
         * 此处排序可以使用匿名实现类去完成比较，返回正数则大于，返回负数则小于，相等则返回0，
         * 也可以使用jdk1.8新特性lambda表达式去完成，此处使用lambda表达式去实现
         *
         * 切两种lambda的实现方法建议使用第二种方式
         * 1.Collections.sort(results, (pair1, pair2) -> pair1.getKey().compareTo(pair2.getKey()));
         * 2.Collections.sort(results, Comparator.comparing(Pair::getKey));
         */
        Collections.sort(results, Comparator.comparing(Pair::getKey));
        return results;
    }

    /**
     * 清除计数线程
     */
    public class CleanCountersThread extends Thread {
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset;

        public CleanCountersThread(int sampleCount, long timeOffset) {
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit() {
            quit = true;
        }

        @Override
        public void run() {
            int passes = 0;
            while (!quit) {
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                /*
                 * "known:": 1:test
                 *           5:test
                 *           60:test
                 *           300:test
                 *           3600:test
                 *           18000:test
                 *           86400:test
                 *
                 * 此处按每个时间片循环，当每个时间片执行结束之后则循环终止
                 */
                while (index < conn.zcard("known:")) {
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }
                    String hash = hashSet.iterator().next();
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(":")));
                    int bprec = (int) Math.floor(prec / 60);
                    if (bprec == 0) {
                        bprec = 1;
                    }
                    if ((passes % bprec) != 0) {
                        continue;
                    }

                    String hkey = "count:" + hash;
                    String cutoff = String.valueOf(((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    List<String> samples = new ArrayList<>(conn.hkeys(hkey));
                    Collections.sort(samples);
                    int remove = bisectRight(samples, cutoff);

                    if (remove != 0) {
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size()) {
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0) {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            } else {
                                conn.unwatch();
                            }
                        }
                    }
                }

                passes++;
                long duration = Math.min((System.currentTimeMillis() + timeOffset) - start, 60000);
                try {
                    sleep(Math.max(60000 - duration, 1000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public int bisectRight(List<String> values, String key) {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }

    /**
     * change ip to score
     *
     * @param ip
     */
    public int ipToScore(String ip) {
        int score = 0;
        if (ip == null) {
            return score;
        }
        for (String s : ip.split("\\.")) {
            /*
             * radix 为转换基数，可用基数为：2 8 10 16
             * 例如：
             *      Integer.parseInt("100", 2) as 4
             *      Integer.parseInt("100", 8) as 64
             *      Integer.parseInt("100", 10) as 100
             *      Integer.parseInt("100", 16) as 256
             */
            score = score * 256 + Integer.parseInt(s, 10);
        }
        return score;
    }

    /**
     * import data to redis
     *
     * @param file
     */
    public void importIpsToRedis(File file) {
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
            /*
             * get all records
             */
            List<CSVRecord> recordList = parser.getRecords();
            Pipeline pipe = conn.pipelined();
            for (int i = 0; i < recordList.size(); i++) {
                /*
                 * 当i大于2时（因为前两行是表头）开始获取数据
                 */
                if (i <= 1) {
                    continue;
                }
                printer("currentNum: " + i);
                /*
                 * .parseInt() method is end at 2147483648
                 */
                int startNum = Integer.parseInt(recordList.get(i).get(0), 10);
                String cityNo = recordList.get(i).get(2);
                pipe.zadd("ip2CityId:", startNum, cityNo + "_" + i);
            }
            pipe.sync();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * import Cities into redis
     *
     * @param file
     */
    public void importCitesToRedis(File file) {
        Gson gson = new Gson();
        FileReader reader = null;
        Pipeline pipe = conn.pipelined();
        try {
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
            List<CSVRecord> recordList = parser.getRecords();
            for (int i = 0; i < recordList.size(); i++) {
                /*
                 * 当i大于2时（因为前两行是表头）开始获取数据
                 */
                if (i <= 1) {
                    continue;
                }
                printer("currentNum: " + i);
                /*
                 * 取出locId, country, region, city，并转成json格式存入redisHash
                 */
                String cityStr = gson.toJson(new String[]{recordList.get(i).get(0), recordList.get(i).get(1),
                        recordList.get(i).get(2), recordList.get(i).get(3)});
                pipe.hset("cityId2City:", recordList.get(i).get(0), cityStr);
            }
            pipe.sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 查询IP所属城市信息
     */
    public void testIpLookup() {
        printer("\n----- testIpLookup -----");
        File blocks = new File("/Users/yangkaiqiang/Documents/data/GeoLiteCity-Blocks.csv");
        File locations = new File("/Users/yangkaiqiang/Documents/data/GeoLiteCity-Location.csv");
        if (!blocks.exists()) {
            printer("*****");
            printer("GeoLiteCity-Blocks.csv file not found...");
            printer("*****");
        }
        if (!locations.exists()) {
            printer("*****");
            printer("GeoLiteCity-Blocks.csv file not found...");
            printer("*****");
        }

        printer("importing IP addresses to Redis...(this may take a while)");
//        importIpsToRedis(blocks);
        long ranges = conn.zcard("ip2CityId:");
        printer("Loaded ranges into Redis: " + ranges);
        assert ranges > 1000;
        printer();

        printer("importing Location lookups into Redis...(this may take a while)");
//        importCitesToRedis(locations);
        long cities = conn.hlen("ip2CityId: ");
        printer("Loaded city lookups into redis: " + cities);
        assert cities > 1000;
        printer();

        printer("Let's lookup some locations!");
        for (int i = 0; i < 10; i++) {
            String ip = randomOctet(255) + '.' +
                    randomOctet(256) + '.' +
                    randomOctet(256) + '.' +
                    randomOctet(256);
            printer(Arrays.toString(findCityByIp(ip)));
        }

    }

    /**
     * 生成指定范围内的随机数
     *
     * @param max
     * @return
     */
    public String randomOctet(int max) {
        return String.valueOf((int) (Math.random() * max));
    }

    /**
     * 通过IP查找城市信息
     *
     * @param ipAddress
     * @return
     */
    public String[] findCityByIp(String ipAddress) {
        int score = ipToScore(ipAddress);
        /*
         * 获取IP分值小于或等于给定分值的城市ID
         */
        Set<String> results = conn.zrevrangeByScore("ip2CityId:", score, 0, 0, 1);
        if (results.size() == 0) {
            return null;
        }

        String cityId = results.iterator().next();
        cityId = cityId.substring(0, cityId.indexOf('_'));
        return new Gson().fromJson(conn.hget("cityId2City:", cityId), String[].class);
    }
}










