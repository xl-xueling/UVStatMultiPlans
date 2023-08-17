package com.dtstep.uvstatmultiplans.plans.plan7;

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
import org.apache.flink.shaded.guava30.com.google.common.hash.Hashing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class UVStat7 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
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
                .trigger(new TimeIntervalTrigger<>(5, TimeUnit.SECONDS))
                .aggregate(new RoaringBitMapAggregate(),new WindowResultFunction())
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


    public static class RoaringBitMapAggregate implements AggregateFunction<UserBehavior, Roaring64Bitmap, Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public Roaring64Bitmap createAccumulator() {
            return new Roaring64Bitmap();
        }

        @Override
        public Roaring64Bitmap add(UserBehavior userBehavior, Roaring64Bitmap roaring64Bitmap) {
            long value = Math.abs(Hashing.murmur3_128().hashBytes(userBehavior.getUserId().getBytes(StandardCharsets.UTF_8)).asLong());
            roaring64Bitmap.add(value);
            return roaring64Bitmap;
        }

        @Override
        public Long getResult(Roaring64Bitmap roaring64Bitmap) {
            return roaring64Bitmap.getLongCardinality();
        }

        @Override
        public Roaring64Bitmap merge(Roaring64Bitmap acc1, Roaring64Bitmap acc2) {
            acc1.and(acc2);
            return acc1;
        }
    }
}
