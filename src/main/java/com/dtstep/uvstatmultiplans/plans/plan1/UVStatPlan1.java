package com.dtstep.uvstatmultiplans.plans.plan1;

import com.dtstep.uvstatmultiplans.common.SysConst;
import com.dtstep.uvstatmultiplans.entity.PageUVResult;
import com.dtstep.uvstatmultiplans.entity.UserBehavior;
import com.dtstep.uvstatmultiplans.trigger.TimeIntervalTrigger;
import com.dtstep.uvstatmultiplans.util.DateUtil;
import com.dtstep.uvstatmultiplans.util.JsonUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Plan1：使用Flink，基于Set内存计数方式实现uv统计
 * Author：XueLing
 * Site：https://dtstep.com
 */
public class UVStatPlan1 {

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
                .aggregate(new UVStatAggregate(),new WindowResultFunction())
                .map(x -> {
                    System.out.println("key:" + x.page + ",window time:" + DateUtil.formatTimeStamp(x.windowTime,"yyyy-MM-dd HH:mm:ss") + ",uv:" + x.uv);
                    return null;
                });
        env.execute();
    }

    public static class WindowResultFunction implements WindowFunction<Integer, PageUVResult, String, TimeWindow> {

        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Integer> aggregateResult,
                Collector<PageUVResult> collector
        ) throws Exception {
            Integer count = aggregateResult.iterator().next();
            collector.collect(PageUVResult.of(key, window.getEnd(), count));
        }
    }

    public static class UVStatAggregate implements AggregateFunction<UserBehavior, Set<String>, Integer> {

        @Override
        public Set<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public Set<String> add(UserBehavior userBehavior, Set<String> accumulator) {
            accumulator.add(userBehavior.getUserId());
            return accumulator;
        }

        @Override
        public Integer getResult(Set<String> accumulator) {
            return accumulator.size();
        }

        @Override
        public Set<String> merge(Set<String> a, Set<String> b) {
            a.addAll(b);
            return a;
        }
    }
}
