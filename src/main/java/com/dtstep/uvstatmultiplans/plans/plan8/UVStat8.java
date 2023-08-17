package com.dtstep.uvstatmultiplans.plans.plan8;

import com.dtstep.uvstatmultiplans.common.SysConst;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.redis.RedisHandler;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Plan8：使用Redis，基于set计算uv
 * Author：XueLing
 * Site：https://dtstep.com
 */
public class UVStat8 {

    private static final int intervalMinutes = 1;

    private static KafkaConsumer<String,String> consumer;

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
                            process(userBehavior);
                        }
                    }
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }finally {
                consumer.close();
            }
        }
    }


    public static void process(UserBehavior userBehavior) throws Exception {
        long time = userBehavior.getBehaviorTime();
        long batchTime = DateUtil.batchTime(intervalMinutes,TimeUnit.MINUTES,time);
        String cacheKey = "cache_uv_"+userBehavior.getPage() + "_" + userBehavior.getUserId() + "_" + batchTime;
        Boolean isExist = userExistCache.getIfPresent(cacheKey);
        if(isExist == null || !isExist){
            userExistCache.put(cacheKey,true);
            String redisKey = "user_"+userBehavior.getPage() + "_" +batchTime;
            RedisHandler.getInstance().sadd(redisKey, Lists.newArrayList(userBehavior.getUserId()),(int)TimeUnit.MINUTES.toSeconds(intervalMinutes * 5));
        }
    }

    public static long getUV(String page,long batchTime) throws Exception {
        String redisKey = "user_" + page + "_" +batchTime;
        return  RedisHandler.getInstance().scard(redisKey);
    }
}
