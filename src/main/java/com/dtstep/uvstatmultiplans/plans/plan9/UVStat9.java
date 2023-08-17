package com.dtstep.uvstatmultiplans.plans.plan9;

import com.dtstep.uvstatmultiplans.common.SysConst;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.redis.RedisHandler;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import com.dtstep.uvstatmultiplans.util.RandomID;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Plan8：使用Redis，基于set计算uv
 * Author：XueLing
 * Site：https://dtstep.com
 */
public class UVStat9 {

    private static final int intervalMinutes = 1;

    private static KafkaConsumer<String,String> consumer;

    private static final String prefix = "uvstat";

    private static final Cache<String,Boolean> userExistCache = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .maximumSize(100000)
            .softValues()
            .build();

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", SysConst.KAFKA_BOOTSTRAP_SERVERS);
        properties.put("group.id","group_" + System.currentTimeMillis());
        properties.put("enable.auto.commit","true");
        properties.put("auto.offset.reset","latest");
        properties.put("kafka.session.timeout.ms", "120000");
        properties.put("kafka.request.timeout.ms","90000");
        properties.put("kafka.allow.auto.create.topics","false");
        properties.put("kafka.max.poll.interval.ms","1500000");
        properties.put("kafka.fetch.max.wait.ms","1000");
        properties.put("kafka.fetch.max.bytes","2097152");
        properties.put("kafka.connections.max.idle.ms","1080000");
        properties.put("kafka.max.partition.fetch.bytes","2097152");
        properties.put("kafka.max.poll.records","300");
        properties.put("kafkaConsumer.pollTimeoutMs", "180000");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(SysConst.KAFKA_TOPIC_NAME));
        ConsumerThread consumerThread = new ConsumerThread();
        consumerThread.run();
    }

    public static class ConsumerThread implements Runnable {

        @Override
        public void run() {
            try{
                ConsumerBehaviorPool consumerBehaviorPool = new ConsumerBehaviorPool();
                while (true) {
                    ConsumerRecords<String, String> messageList = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> message : messageList) {
                        UserBehavior userBehavior = null;
                        try{
                            userBehavior = JsonUtil.toJavaObject(message.value(),UserBehavior.class);
                        }catch (Exception ex){
                            ex.printStackTrace();
                        }
                        if(userBehavior != null){
                            consumerBehaviorPool.addBehavior(userBehavior);
                        }
                    }
                    if(consumerBehaviorPool.size() > 50 || System.currentTimeMillis() - consumerBehaviorPool.processTime > TimeUnit.SECONDS.toMillis(5)){
                        process(consumerBehaviorPool.take());
                    }
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }finally {
                consumer.close();
            }
        }
    }

    public static class ConsumerBehaviorPool {

        private final List<UserBehavior> userBehaviorList = new ArrayList<>();

        private long processTime = System.currentTimeMillis();

        public long getProcessTime() {
            return processTime;
        }

        public void setProcessTime(long processTime) {
            this.processTime = processTime;
        }

        public void addBehavior(UserBehavior userBehavior){
            this.userBehaviorList.add(userBehavior);
        }

        public void clear(){
            this.userBehaviorList.clear();
        }

        public int size(){
            return this.userBehaviorList.size();
        }

        public List<UserBehavior> take(){
            List<UserBehavior> copyList = new ArrayList<>(userBehaviorList);
            clear();
            setProcessTime(System.currentTimeMillis());
            return copyList;
        }
    }

    public static void process(List<UserBehavior> userBehaviors) throws Exception {
        Map<String,List<UserBehavior>> eventMap = userBehaviors.stream().collect(Collectors.groupingBy(x -> x.getPage() + "#" + DateUtil.batchTime(intervalMinutes,TimeUnit.MINUTES,x.getBehaviorTime())));
        for(String k : eventMap.keySet()){
            String page = k.split("#")[0];
            long batchTime = Long.parseLong(k.split("#")[1]);
            List<UserBehavior> userBehaviorList = eventMap.get(k);
            List<String> userIds = new ArrayList<>();
            for(UserBehavior userBehavior : userBehaviorList){
                String cacheKey = "cache_uv_"+page + "_" + userBehavior.getUserId() + "_" + batchTime;
                Boolean isExist = userExistCache.getIfPresent(cacheKey);
                if(isExist == null || !isExist){
                    userExistCache.put(cacheKey,true);
                    userIds.add(userBehavior.getUserId());
                }
            }
            String redisKey = getRedisKey(page,batchTime);
            RedisHandler.getInstance().sadd(redisKey,userIds,(int)TimeUnit.MINUTES.toSeconds(intervalMinutes * 5));
            long uv = getUV(page,batchTime);
            System.out.println("process page:" + page + ",batchTime:" + batchTime + ",result:" + uv);
        }
    }

    public static long getUV(String page,long batchTime) throws Exception {
        String redisKey = getRedisKey(page,batchTime);
        return  RedisHandler.getInstance().scard(redisKey);
    }

    public static String getRedisKey(String page,long batchTime){
        return prefix + "_" + page + "_" +batchTime;
    }
}
