package com.dtstep.uvstatmultiplans.plans.plan6;

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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava30.com.google.common.hash.Hashing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Plan5：使用Flink，基于RoaringBitmap实现uv统计
 * Author：XueLing
 * Site：https://dtstep.com
 * GitHub：https://github.com/xl-xueling/UVStatMultiPlans.git
 */
@Deprecated
public class UVStatPlan6 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.getConfig().setAutoWatermarkInterval(1000L);
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
                .trigger(new TimeIntervalTrigger<>(5, TimeUnit.SECONDS))
                .aggregate(new BitsetAggregate(),new WindowResultFunction())
                .map(x -> {
                    System.out.println("key:" + x.page + ",window time:" + DateUtil.formatTimeStamp(x.windowTime,"yyyy-MM-dd HH:mm:ss") + ",uv:" + x.uv);
                    return null;
                });
        env.execute();
    }


    public static class WindowResultFunction implements WindowFunction<Long, PageUVResult, String, TimeWindow> {

        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Long> aggregateResult,
                Collector<PageUVResult> collector
        ) throws Exception {
            Long count = aggregateResult.iterator().next();
            collector.collect(PageUVResult.of(key, window.getEnd(), count));
        }
    }


    public static class BitsetAggregate implements AggregateFunction<UserBehavior, Tuple2<BitSet, BitSet>, Long> {

        private static final long serialVersionUID = 1L;

        private long uv;

        @Override
        public Tuple2<BitSet, BitSet> createAccumulator() {
            return Tuple2.of(new BitSet(),new BitSet());
        }

        @Override
        public Tuple2<BitSet, BitSet> add(UserBehavior userBehavior, Tuple2<BitSet, BitSet> bitSetTuple2) {
            long bitIndex = Math.abs(Hashing.murmur3_128().hashBytes(userBehavior.getUserId().getBytes(StandardCharsets.UTF_8)).asLong());
            int low = (int) (bitIndex);
            int high = (int) (bitIndex >>> 32);
            if(low < 0){low = ~low;}
            if(high < 0){high = ~high;}
             if(!bitSetTuple2.f0.get(low) || !bitSetTuple2.f1.get(high)){
                 bitSetTuple2.f0.set(low);
                 bitSetTuple2.f1.set(high);
                 uv++;
             }
            return bitSetTuple2;
        }

        @Override
        public Long getResult(Tuple2<BitSet, BitSet> bitsetTuple2) {
            return uv;
        }

        @Override
        public Tuple2<BitSet, BitSet> merge(Tuple2<BitSet, BitSet> acc1, Tuple2<BitSet, BitSet> acc2) {
            acc1.f0.and(acc2.f0);
            acc1.f1.and(acc2.f1);
            return acc1;
        }
    }
}
