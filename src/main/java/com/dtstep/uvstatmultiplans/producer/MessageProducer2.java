package com.dtstep.uvstatmultiplans.producer;

import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.source.KafkaSender;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 向Kafka模拟发送消息
 */
public class MessageProducer2 {

    private static final ScheduledExecutorService service = Executors.newScheduledThreadPool(2,
            new BasicThreadFactory.Builder().namingPattern("schedule-pool-%d").daemon(true).build());

    public static void main(String[] args) throws Exception {
        long t = System.currentTimeMillis();
        for(int j=0;j<10;j++){
            for(int n=0;n<3;n++){
                for(int i=0;i<100;i++){
                    String userId = UUID.randomUUID().toString();
                    UserBehavior userBehavior = new UserBehavior();
                    userBehavior.setUserId(userId);
                    userBehavior.setPage("page_"+n);
                    userBehavior.setBehaviorTime(t);
                    String message = JsonUtil.toJSONString(userBehavior);
                    KafkaSender.send(message);
                    Thread.sleep(3);
                }
            }
            Thread.sleep(1000);
            System.out.println("send batch:" + j);
        }

        System.out.println("send ok!");
        Thread.currentThread().join();
    }

}
