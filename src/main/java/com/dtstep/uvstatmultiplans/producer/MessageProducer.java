package com.dtstep.uvstatmultiplans.producer;

import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import com.dtstep.uvstatmultiplans.source.KafkaSender;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 向Kafka模拟发送消息
 */
public class MessageProducer {

    private static final ScheduledExecutorService service = Executors.newScheduledThreadPool(2,
            new BasicThreadFactory.Builder().namingPattern("schedule-pool-%d").daemon(true).build());

    public static void main(String[] args) throws Exception {
        service.scheduleWithFixedDelay(new Sender(),0,10, TimeUnit.SECONDS);
        Thread.currentThread().join();
    }

    private static class Sender extends Thread {

        @Override
        public void run() {
            long t = System.currentTimeMillis();
            System.out.println("start send message,batch:"+ DateUtil.formatTimeStamp(t,"yyyy-MM-dd HH:mm:ss"));
            for(int i=0;i<1;i++){
                String page = "page_"+i;
                for(int j=0;j<500;j++){
                    String userId = UUID.randomUUID().toString();
                    UserBehavior userBehavior = new UserBehavior();
                    userBehavior.setUserId(userId);
                    userBehavior.setPage(page);
                    userBehavior.setBehaviorTime(t);
                    String message = JsonUtil.toJSONString(userBehavior);
                    System.out.println(message);
                    //KafkaSender.send(message);
                }
            }
        }
    }
}
