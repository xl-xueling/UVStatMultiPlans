package com.dtstep.uvstatmultiplans.test.redis;

import com.dtstep.uvstatmultiplans.redis.RedisHandler;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class RedisClientTest {

    @Test
    public void testPfCount() throws Exception {
        for(int i=0;i<100;i++){
            String key = "key:"+i;
            RedisHandler.getInstance().pfadd("abc",key,(int) TimeUnit.MINUTES.toSeconds(30));
        }
        long value = RedisHandler.getInstance().pfcount("abc");
        System.out.println("value:" + value);
    }
}
