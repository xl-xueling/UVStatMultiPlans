package com.dtstep.uvstatmultiplans.redis;
/*
 * Copyright (C) 2022-2023 XueLing.雪灵
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import jodd.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public final class RedisClient {

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private static JedisCluster jedisCluster = null;

    public void init(String[] servers) {
        init(servers, null);
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void del(String key){
        jedisCluster.del(key);
    }

    public Set<Tuple> zrevrange(String key, int start, int end){
        return jedisCluster.zrevrangeWithScores(key, start, end);
    }

    public void pfadd(String key,String value,int seconds){
        jedisCluster.pfadd(key,value);
        jedisCluster.expire(key,seconds);
    }

    public void pfadd(String key,List<String> values,int seconds){
        if(CollectionUtils.isEmpty(values)){
            return;
        }
        jedisCluster.pfadd(key, values.toArray(new String[0]));
        jedisCluster.expire(key,seconds);
    }

    public void pfmerge(String key1,String key2){
        jedisCluster.pfmerge(key1,key2);
    }

    public long pfcount(String key){
        return jedisCluster.pfcount(key);
    }

    public Set<Tuple> zrange(String key,int start,int end){
        return jedisCluster.zrangeWithScores(key, start, end);
    }

    public List<String> lrange(String key,int start,int end){
        return jedisCluster.lrange(key, start, end);
    }

    public synchronized void init(String[] servers, String password) {
        GenericObjectPoolConfig<Jedis> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(1000);
        config.setMinIdle(200);
        config.setMaxWaitMillis(20000);
        config.setTestOnBorrow(false);
        config.setTestOnReturn(true);
        config.setTestOnCreate(false);
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        if (ArrayUtils.isNotEmpty(servers)) {
            for (String sever : servers) {
                jedisClusterNodes.add(new HostAndPort(sever.split(":")[0], Integer.parseInt(sever.split(":")[1])));
            }
        }
        int timeout = 20000;
        int maxAttempts = 10;
        int soTimeout = 10000;
        if (password == null || password.length() == 0) {
            jedisCluster = new JedisCluster(jedisClusterNodes, timeout, maxAttempts, config);
        } else {
            jedisCluster = new JedisCluster(jedisClusterNodes, timeout, soTimeout, maxAttempts, password, config);
        }
    }

    public void set(final String key,String value,final int expireSeconds) {
        if(StringUtil.isEmpty(key) || StringUtil.isEmpty(value)){
            return;
        }
        jedisCluster.setex(key, expireSeconds, value);
    }

    public void setBytes(final String key,byte[] value,final int expireSeconds) {
        if(StringUtil.isEmpty(key) || value == null){
            return;
        }
        jedisCluster.setex(key.getBytes(),expireSeconds,value);
    }

    public void sadd(String key,List<String> values,int second){
        if(CollectionUtils.isEmpty(values)){
            return;
        }
        jedisCluster.sadd(key,values.toArray(new String[0]));
        jedisCluster.expire(key,second);
    }

    public long scard(String key){
        return jedisCluster.scard(key);
    }

    public byte[] getBytes(final String key) {
        if(StringUtil.isEmpty(key)){
            return null;
        }
        return jedisCluster.get(key.getBytes());
    }

    public String get(final String key) {
        return jedisCluster.get(key);
    }

    public boolean exist(final String key) {
        return jedisCluster.exists(key);
    }

    public void expire(final String key,int second) throws Exception{
        jedisCluster.expire(key,second);
    }

    public void incrBy(final String key,int step) throws Exception{
        jedisCluster.incrBy(key,step);
    }

    @Deprecated
    public void batchDelete(String hashTag,String prefix){
        ScanParams scanParams = new ScanParams().match(hashTag.concat(prefix).concat("*")).count(200);
        String cur = ScanParams.SCAN_POINTER_START;
        boolean hasNext = true;
        while (hasNext) {
            ScanResult<String> scanResult = jedisCluster.scan(cur, scanParams);
            List<String> keys = scanResult.getResult();
            for (String key : keys) {
                jedisCluster.del(key);
            }
            cur = scanResult.getCursor();
            if (StringUtils.equals("0", cur)) {
                hasNext = false;
            }
        }
    }
}
