package com.dtstep.uvstatmultiplans.plans.plan5;

import com.dtstep.uvstatmultiplans.common.SysConst;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.plans.plan2.BaseStateProcessWindowFunction;
import com.dtstep.uvstatmultiplans.trigger.TimeIntervalTrigger;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Plan2：使用Flink，基于State实现uv统计
 * Author：XueLing
 * Site：https://dtstep.com
 */
public class UVStatPlan5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(TimeUnit.MINUTES.toMillis(10));
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", SysConst.KAFKA_BOOTSTRAP_SERVERS);
        kafkaProperties.setProperty("group.id","groupId_" + System.currentTimeMillis());
        kafkaProperties.setProperty("auto.offset.reset","latest");
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(SysConst.KAFKA_TOPIC_NAME, new SimpleStringSchema(), kafkaProperties);
        DataStream<UserBehavior> dataStream = env.addSource(consumer).map(x -> {
            UserBehavior userBehavior = null;
            try{
                userBehavior = JsonUtil.toJavaObject(x,UserBehavior.class);
            }catch (Exception ex){
                ex.printStackTrace();
            }
            return userBehavior;
        }).assignTimestampsAndWatermarks
                (WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>)
                        (userBehavior, l) -> userBehavior.getBehaviorTime()));
        dataStream.keyBy((KeySelector<UserBehavior, String>) UserBehavior::getPage).window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new TimeIntervalTrigger<>(5,TimeUnit.SECONDS))
                .process(new HyperLogProcessWindowFunction())
                .map(x -> {
                    System.out.println("key:" + x.page + ",window time:" + DateUtil.formatTimeStamp(x.windowTime,"yyyy-MM-dd HH:mm:ss") + ",uv:" + x.uv);
                    return null;
                });

        env.execute();
    }
}
